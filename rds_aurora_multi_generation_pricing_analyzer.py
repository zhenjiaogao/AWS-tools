#!/usr/bin/env python3
"""
支持获取指定区域的RDS MySQL实例信息，并计算RDS和Aurora的RI定价
Aurora转换支持r7g、r8g两种实例类型，分别计算实例成本、存储成本和总MRR成本
RDS替换支持m7g、m8g、r7g、r8g四种实例类型，分别计算实例成本、存储成本和总MRR成本

支持的实例类型：
1. M系列实例：迁移Graviton3/4时使用m7g、m8g，迁移Aurora时使用r7g、r8g
2. R系列实例：迁移Graviton3/4时使用r7g、r8g，迁移Aurora时使用r7g、r8g
3. C系列实例：迁移Graviton3/4时使用m7g、m8g，迁移Aurora时使用r7g、r8g

Multi-AZ存储成本优化：
1. 当RDS MySQL是Multi-AZ架构时，迁移到Aurora时，存储成本不乘以2，仅算一份
2. 当Multi-AZ架构并且有多个read replica时，迁移到Aurora的存储成本计算到Primary节点上，仅计算一份
3. RDS替换场景下，Multi-AZ存储成本仍然乘以2（保持RDS原有逻辑）

RDS MySQL集群支持：
1. 通过source_db_instance_identifier识别RDS MySQL集群关系
2. 当read replica的source_db_instance_identifier值和primary的db_instance_identifier值相同时，识别为集群
3. 迁移到Aurora时，集群的存储成本仅计算一次，并加和到Primary节点上
4. Read replica节点的Aurora存储成本为0，避免重复计算
5. 支持复杂的集群拓扑，包括一个primary对应多个read replica的场景

Aurora存储容量优化：
1. aurora_allocate_storage_gb 基于 Primary节点的 used_storage_gb（实际使用量）计算
2. 符合Aurora共享存储特性，不累加集群中所有节点的存储量
3. 更准确反映迁移到Aurora后的实际存储需求和成本
4. 优先使用Primary节点的实际使用量，如果未获取到则回退到分配量
5. 避免基于过度分配或重复计算的存储容量进行成本计算

提升执行效率：
在脚本中实现了并发获取 cloudwatch 信息等，使用 --optimize 启用并发处理，-b -w 指定 batch size 以及并发数
"""

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd


class CompleteUnifiedRDSAuroraAnalyzer:
    def __init__(self, region: str, enable_optimization: bool = False, max_workers: int = 10, batch_size: int = 50):
        """
        完整统一分析器，包含原始脚本的所有功能
        """
        self.region = region
        self.enable_optimization = enable_optimization
        self.max_workers = max_workers if enable_optimization else 1
        self.batch_size = batch_size
        
        # AWS客户端
        self.rds_client = boto3.client('rds', region_name=region)
        self.pricing_client = boto3.client('pricing', region_name='us-east-1')
        self.sts_client = boto3.client('sts', region_name=region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        
        # 缓存
        self.pricing_cache = {}
        self.aurora_pricing_cache = {}
        self.storage_pricing_cache = {}
        self.aurora_storage_pricing_cache = {}
        
        # 实例类型配置
        self.aurora_generations = ['r7g', 'r8g']
        self.rds_replacement_generations = ['m7g', 'm8g', 'r7g', 'r8g']
        
        self._setup_logging()

    def _setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{self.region}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _print_progress(self, message: str):
        """打印进度信息"""
        print(message)
        self.logger.info(message)

    def get_account_id(self) -> str:
        """获取AWS账户ID"""
        try:
            response = self.sts_client.get_caller_identity()
            return response['Account']
        except Exception as e:
            self.logger.error(f"获取账户ID失败: {e}")
            return "unknown"

    def _get_pricing_location(self) -> str:
        """获取定价API的区域名称"""
        region_mapping = {
            "us-east-1": "US East (N. Virginia)",
            "us-east-2": "US East (Ohio)",
            "us-west-1": "US West (N. California)",
            "us-west-2": "US West (Oregon)",
            "ap-east-1": "Asia Pacific (Hong Kong)",
            "ap-south-1": "Asia Pacific (Mumbai)",
            "ap-south-2": "Asia Pacific (Hyderabad)",
            "ap-northeast-1": "Asia Pacific (Tokyo)",
            "ap-northeast-2": "Asia Pacific (Seoul)",
            "ap-northeast-3": "Asia Pacific (Osaka)",
            "ap-southeast-1": "Asia Pacific (Singapore)",
            "ap-southeast-2": "Asia Pacific (Sydney)",
            "ap-southeast-3": "Asia Pacific (Jakarta)",
            "ap-southeast-4": "Asia Pacific (Melbourne)",
            "ca-central-1": "Canada (Central)",
            "eu-central-1": "EU (Frankfurt)",
            "eu-central-2": "EU (Zurich)",
            "eu-west-1": "EU (Ireland)",
            "eu-west-2": "EU (London)",
            "eu-west-3": "EU (Paris)",
            "eu-north-1": "EU (Stockholm)",
            "eu-south-1": "EU (Milan)",
            "eu-south-2": "EU (Spain)",
            "me-south-1": "Middle East (Bahrain)",
            "me-central-1": "Middle East (UAE)",
            "af-south-1": "Africa (Cape Town)",
            "sa-east-1": "South America (Sao Paulo)"
        }
        return region_mapping.get(self.region, self.region)

    def _should_process_instance(self, instance_class: str) -> bool:
        """判断是否应该处理该实例类型"""
        if not instance_class.startswith('db.'):
            return False
        
        instance_family = instance_class.split('.')[1] if len(instance_class.split('.')) > 1 else ''
        
        if instance_family.startswith(('m', 'r', 'c')):
            return True
        else:
            self.logger.warning(f"未知实例族类型: {instance_class}，将继续处理")
            return True

    def get_storage_info(self, db_instance_identifier: str, db_instance_data: Dict) -> Dict:
        """获取存储信息"""
        storage_info = {
            'allocated_storage_gb': db_instance_data.get('AllocatedStorage', 0),
            'storage_type': db_instance_data.get('StorageType', 'gp2'),
            'iops': db_instance_data.get('Iops', 0),
            'storage_throughput': db_instance_data.get('StorageThroughput', 0),
            'used_storage_gb': 0,
            'free_storage_gb': 0,
            'storage_utilization_percent': 0
        }
        
        # 获取CloudWatch指标
        try:
            used_storage = self._get_used_storage_from_cloudwatch(db_instance_identifier)
            if used_storage is not None:
                storage_info['used_storage_gb'] = used_storage
                storage_info['free_storage_gb'] = max(0, storage_info['allocated_storage_gb'] - used_storage)
                if storage_info['allocated_storage_gb'] > 0:
                    storage_info['storage_utilization_percent'] = (used_storage / storage_info['allocated_storage_gb']) * 100
        except Exception as e:
            self.logger.warning(f"获取CloudWatch指标失败: {e}")
        
        self.logger.info(f"实例 {db_instance_identifier} 存储信息: 类型={storage_info['storage_type']}, "
                        f"分配={storage_info['allocated_storage_gb']}GB, "
                        f"剩余={storage_info['free_storage_gb']}GB, "
                        f"IOPS={storage_info['iops']}, "
                        f"吞吐量={storage_info['storage_throughput']}MB/s")
        
        return storage_info

    def _get_used_storage_from_cloudwatch(self, db_instance_identifier: str) -> Optional[float]:
        """从CloudWatch获取已使用存储空间"""
        try:
            end_time = time.time()
            start_time = end_time - 86400  # 24小时前
            
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='FreeStorageSpace',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average']
            )
            
            if response['Datapoints']:
                # 获取最新的空闲存储空间（字节）
                latest_free_bytes = min(dp['Average'] for dp in response['Datapoints'])
                
                # 获取分配的存储空间
                db_response = self.rds_client.describe_db_instances(
                    DBInstanceIdentifier=db_instance_identifier
                )
                allocated_gb = db_response['DBInstances'][0]['AllocatedStorage']
                allocated_bytes = allocated_gb * 1024 * 1024 * 1024
                
                # 计算已使用存储空间
                used_bytes = allocated_bytes - latest_free_bytes
                used_gb = used_bytes / (1024 * 1024 * 1024)
                
                return max(0, used_gb)
            
            return None
            
        except Exception as e:
            self.logger.warning(f"获取CloudWatch存储指标失败: {e}")
            return None

    def get_cloudwatch_metrics(self, db_instance_identifier: str) -> Dict:
        """获取CloudWatch指标"""
        metrics = {
            'iops-avg.1Hr/15-day period': 0,
            'cpu-max.1Hr/15-day period': 0,
            'cpu-min.1Hr/15-day period': 0,
            'cpu-avg.1Hr/15-day period': 0
        }
        
        try:
            # 15天前到现在的时间范围
            end_time = time.time()
            start_time = end_time - (15 * 24 * 3600)  # 15天前
            
            # IOPS指标（ReadIOPS + WriteIOPS）
            try:
                read_iops_response = self.cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName='ReadIOPS',
                    Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Average']
                )
                
                write_iops_response = self.cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName='WriteIOPS',
                    Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Average']
                )
                
                # 计算总IOPS平均值
                if read_iops_response['Datapoints'] and write_iops_response['Datapoints']:
                    # 按时间戳对齐数据点
                    read_dict = {dp['Timestamp']: dp['Average'] for dp in read_iops_response['Datapoints']}
                    write_dict = {dp['Timestamp']: dp['Average'] for dp in write_iops_response['Datapoints']}
                    
                    total_iops_values = []
                    for timestamp in read_dict:
                        if timestamp in write_dict:
                            total_iops_values.append(read_dict[timestamp] + write_dict[timestamp])
                    
                    if total_iops_values:
                        metrics['iops-avg.1Hr/15-day period'] = round(sum(total_iops_values) / len(total_iops_values), 2)
                        
            except Exception as e:
                self.logger.warning(f"获取IOPS指标失败: {e}")
            
            # CPU指标
            cpu_response = self.cloudwatch_client.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average', 'Maximum', 'Minimum']
            )
            
            if cpu_response['Datapoints']:
                cpu_values = [dp['Average'] for dp in cpu_response['Datapoints']]
                metrics['cpu-avg.1Hr/15-day period'] = sum(cpu_values) / len(cpu_values)
                metrics['cpu-max.1Hr/15-day period'] = max(dp['Maximum'] for dp in cpu_response['Datapoints'])
                metrics['cpu-min.1Hr/15-day period'] = min(dp['Minimum'] for dp in cpu_response['Datapoints'])
            
        except Exception as e:
            self.logger.warning(f"获取CloudWatch指标失败: {e}")
        
        return metrics
    def get_all_rds_mysql_pricing(self) -> None:
        """获取所有RDS MySQL定价"""
        if self.pricing_cache:
            self.logger.info("RDS MySQL定价缓存已存在，跳过重复获取")
            return
        
        try:
            self.logger.info(f"开始一次性获取区域 {self.region} 的所有RDS MySQL实例定价...")
            
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'servicecode', 'Value': 'AmazonRDS'},
                {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'},
                {'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Single-AZ'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_pricing_location()}
            ]
            
            paginator = self.pricing_client.get_paginator('get_products')
            page_iterator = paginator.paginate(ServiceCode='AmazonRDS', Filters=filters)
            
            processed_count = 0
            for page in page_iterator:
                for price_item in page['PriceList']:
                    product = json.loads(price_item)
                    attributes = product.get('product', {}).get('attributes', {})
                    instance_type = attributes.get('instanceType')
                    
                    if not instance_type or not self._should_process_instance(instance_type):
                        continue
                    
                    terms = product.get('terms', {})
                    reserved_terms = terms.get('Reserved', {})
                    
                    for term_key, term_data in reserved_terms.items():
                        term_attributes = term_data.get('termAttributes', {})
                        if (term_attributes.get('LeaseContractLength') == '1yr' and 
                            term_attributes.get('PurchaseOption') == 'No Upfront'):
                            
                            price_dimensions = term_data.get('priceDimensions', {})
                            for price_key, price_data in price_dimensions.items():
                                price_per_unit = price_data.get('pricePerUnit', {})
                                usd_price = price_per_unit.get('USD')
                                if usd_price:
                                    hourly_rate = float(usd_price)
                                    self.pricing_cache[instance_type] = hourly_rate
                                    processed_count += 1
                                    self.logger.debug(f"缓存RDS MySQL定价: {instance_type} = ${hourly_rate}/小时")
                                    break
                            break
            
            self.logger.info(f"RDS MySQL定价缓存完成，共缓存 {processed_count} 个实例类型的定价信息")
            
        except Exception as e:
            self.logger.error(f"获取RDS MySQL定价失败: {e}")

    def get_all_aurora_mysql_pricing(self) -> None:
        """获取所有Aurora MySQL定价"""
        if self.aurora_pricing_cache:
            self.logger.info("Aurora MySQL定价缓存已存在，跳过重复获取")
            return
        
        try:
            self.logger.info(f"开始一次性获取区域 {self.region} 的所有Aurora MySQL实例定价...")
            
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'servicecode', 'Value': 'AmazonRDS'},
                {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'Aurora MySQL'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_pricing_location()}
            ]
            
            paginator = self.pricing_client.get_paginator('get_products')
            page_iterator = paginator.paginate(ServiceCode='AmazonRDS', Filters=filters)
            
            processed_count = 0
            for page in page_iterator:
                for price_item in page['PriceList']:
                    product = json.loads(price_item)
                    attributes = product.get('product', {}).get('attributes', {})
                    instance_type = attributes.get('instanceType')
                    
                    if not instance_type:
                        continue
                    
                    terms = product.get('terms', {})
                    reserved_terms = terms.get('Reserved', {})
                    
                    for term_key, term_data in reserved_terms.items():
                        term_attributes = term_data.get('termAttributes', {})
                        if (term_attributes.get('LeaseContractLength') == '1yr' and 
                            term_attributes.get('PurchaseOption') == 'No Upfront'):
                            
                            price_dimensions = term_data.get('priceDimensions', {})
                            for price_key, price_data in price_dimensions.items():
                                price_per_unit = price_data.get('pricePerUnit', {})
                                usd_price = price_per_unit.get('USD')
                                if usd_price:
                                    hourly_rate = float(usd_price)
                                    self.aurora_pricing_cache[instance_type] = hourly_rate
                                    processed_count += 1
                                    self.logger.debug(f"缓存Aurora MySQL定价: {instance_type} = ${hourly_rate}/小时")
                                    break
                            break
            
            self.logger.info(f"Aurora MySQL定价缓存完成，共缓存 {processed_count} 个实例类型的定价信息")
            
        except Exception as e:
            self.logger.error(f"获取Aurora MySQL定价失败: {e}")

    def get_all_storage_pricing(self) -> None:
        """获取所有存储类型定价（采用原始脚本的可靠方法）"""
        if self.storage_pricing_cache:
            self.logger.info("存储定价缓存已存在，跳过重复获取")
            return
        
        try:
            self.logger.info(f"开始获取区域 {self.region} 的所有存储类型定价...")
            
            # 支持的存储类型映射（与原始脚本一致）
            storage_type_mapping = {
                'General Purpose': 'gp2',
                'General Purpose-GP3': 'gp3', 
                'Provisioned IOPS': 'io1',
                'Provisioned IOPS-IO2': 'io2',
                'Magnetic': 'magnetic'
            }
            
            # 构建基础过滤器 - 获取所有RDS MySQL存储定价
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'servicecode', 'Value': 'AmazonRDS'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_pricing_location()},
                {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': 'Database Storage'},
                {'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Single-AZ'},
                {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'}
            ]
            
            # 使用分页获取所有存储产品
            paginator = self.pricing_client.get_paginator('get_products')
            page_iterator = paginator.paginate(
                ServiceCode='AmazonRDS',
                Filters=filters
            )
            
            # 初始化所有存储类型的定价信息
            for storage_type in storage_type_mapping.values():
                self.storage_pricing_cache[storage_type] = {
                    'storage_gb_month': 0,
                    'iops_month': 0,
                    'throughput_mbps_month': 0
                }
            
            processed_count = 0
            for page in page_iterator:
                for price_item in page['PriceList']:
                    product = json.loads(price_item)
                    attributes = product.get('product', {}).get('attributes', {})
                    
                    # 获取存储类型
                    volume_type = attributes.get('volumeType')
                    if not volume_type or volume_type not in storage_type_mapping:
                        continue
                    
                    storage_type = storage_type_mapping[volume_type]
                    
                    # 获取按需定价
                    terms = product.get('terms', {})
                    on_demand_terms = terms.get('OnDemand', {})
                    
                    for term_key, term_data in on_demand_terms.items():
                        price_dimensions = term_data.get('priceDimensions', {})
                        for price_key, price_data in price_dimensions.items():
                            price_per_unit = price_data.get('pricePerUnit', {})
                            usd_price = price_per_unit.get('USD')
                            unit = price_data.get('unit', '')
                            
                            if usd_price and float(usd_price) > 0:
                                price_value = float(usd_price)
                                
                                # 根据单位确定价格类型
                                if 'GB-Mo' in unit:
                                    self.storage_pricing_cache[storage_type]['storage_gb_month'] = price_value
                                elif 'IOPS-Mo' in unit:
                                    self.storage_pricing_cache[storage_type]['iops_month'] = price_value
                                elif 'MBps-Mo' in unit or 'MB/s-Mo' in unit:
                                    self.storage_pricing_cache[storage_type]['throughput_mbps_month'] = price_value
                                
                                processed_count += 1
            
            # 获取io1和io2的IOPS定价（使用专门的方法）
            self._get_iops_pricing_for_provisioned_storage()
            
            # 记录缓存结果
            for storage_type, pricing in self.storage_pricing_cache.items():
                if pricing['storage_gb_month'] > 0:
                    self.logger.info(f"存储类型 {storage_type} 定价: "
                                   f"存储=${pricing['storage_gb_month']}/GB/月, "
                                   f"IOPS=${pricing['iops_month']}/IOPS/月, "
                                   f"吞吐量=${pricing['throughput_mbps_month']}/MB/s/月")
            
            self.logger.info(f"存储定价缓存完成，共处理 {processed_count} 个定价项")
            
        except Exception as e:
            self.logger.error(f"获取存储定价失败: {e}")

    def _get_iops_pricing_for_provisioned_storage(self) -> None:
        """专门获取io1和io2存储类型的IOPS定价（采用原始脚本的可靠方法）"""
        self.logger.info("开始获取io1和io2的MySQL IOPS定价...")
        
        mysql_results = {}
        general_results = {}
        
        try:
            # 使用分页器获取所有Provisioned IOPS产品
            paginator = self.pricing_client.get_paginator('get_products')
            for page in paginator.paginate(
                ServiceCode='AmazonRDS',
                Filters=[
                    {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': 'Provisioned IOPS'},
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_pricing_location()}
                ]
            ):
                for product_str in page['PriceList']:
                    product = json.loads(product_str)
                    attrs = product['product']['attributes']
                    
                    usage_type = attrs.get('usagetype', '')
                    
                    # 跳过Multi-AZ和Mirror类型，确保Single-AZ
                    if 'Multi-AZ' in usage_type or 'Mirror' in usage_type:
                        continue
                    
                    terms = product.get('terms', {}).get('OnDemand', {})
                    for term_data in terms.values():
                        for price_data in term_data.get('priceDimensions', {}).values():
                            unit = price_data.get('unit', '')
                            desc = price_data.get('description', '')
                            
                            if 'IOPS' not in unit:
                                continue
                                
                            price_per_unit = price_data.get('pricePerUnit', {})
                            if 'USD' not in price_per_unit:
                                continue
                                
                            price = float(price_per_unit['USD'])
                            
                            # 确定存储类型
                            if 'io1' in desc.lower():
                                storage_type = 'io1'
                            elif 'io2' in desc.lower():
                                storage_type = 'io2'
                            else:
                                continue
                            
                            # 检查是否为MySQL专用定价
                            if 'MySQL' in desc:
                                if storage_type not in mysql_results or price < mysql_results[storage_type]:
                                    mysql_results[storage_type] = price
                                    self.logger.info(f"MySQL专用IOPS定价: {storage_type} ${price}")
                            
                            # 所有Single-AZ IOPS定价都记录为通用定价
                            if storage_type not in general_results or price < general_results[storage_type]:
                                general_results[storage_type] = price
            
            # 合并结果：优先使用MySQL专用定价，否则使用通用定价
            for storage_type in ['io1', 'io2']:
                final_price = None
                
                if storage_type in mysql_results:
                    final_price = mysql_results[storage_type]
                    self.logger.info(f"使用MySQL专用{storage_type} IOPS定价: ${final_price}/IOPS/月")
                elif storage_type in general_results:
                    final_price = general_results[storage_type]
                    self.logger.info(f"使用通用{storage_type} IOPS定价: ${final_price}/IOPS/月")
                
                if final_price:
                    # 确保存储类型已初始化
                    if storage_type not in self.storage_pricing_cache:
                        self.storage_pricing_cache[storage_type] = {
                            'storage_gb_month': 0,
                            'iops_month': 0,
                            'throughput_mbps_month': 0
                        }
                    
                    self.storage_pricing_cache[storage_type]['iops_month'] = final_price
                
        except Exception as e:
            self.logger.error(f"获取MySQL IOPS定价失败: {e}")

    def get_aurora_storage_pricing(self) -> Dict:
        """获取Aurora存储定价"""
        if self.aurora_storage_pricing_cache:
            self.logger.info("Aurora存储定价缓存已存在，跳过重复获取")
            return self.aurora_storage_pricing_cache
        
        try:
            self.logger.info(f"开始获取区域 {self.region} 的Aurora存储定价...")
            
            # 获取Aurora存储定价
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'servicecode', 'Value': 'AmazonRDS'},
                {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'Aurora MySQL'},
                {'Type': 'TERM_MATCH', 'Field': 'usagetype', 'Value': 'Aurora:StorageUsage'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_pricing_location()}
            ]
            
            response = self.pricing_client.get_products(
                ServiceCode='AmazonRDS',
                Filters=filters,
                MaxResults=10
            )
            
            storage_price = 0.1  # 默认值
            for price_item in response['PriceList']:
                product = json.loads(price_item)
                terms = product.get('terms', {})
                on_demand_terms = terms.get('OnDemand', {})
                
                for term_key, term_data in on_demand_terms.items():
                    price_dimensions = term_data.get('priceDimensions', {})
                    for price_key, price_data in price_dimensions.items():
                        price_per_unit = price_data.get('pricePerUnit', {})
                        usd_price = price_per_unit.get('USD')
                        if usd_price:
                            storage_price = float(usd_price)
                            break
                    break
                break
            
            self.logger.info(f"Aurora存储单价: ${storage_price}/GB/月")
            
            # 获取Aurora IO定价
            self.logger.info("通过API获取Aurora IO定价...")
            io_price = 0.2  # 默认值
            
            try:
                filters = [
                    {'Type': 'TERM_MATCH', 'Field': 'servicecode', 'Value': 'AmazonRDS'},
                    {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'Aurora MySQL'},
                    {'Type': 'TERM_MATCH', 'Field': 'usagetype', 'Value': 'Aurora:StorageIOUsage'},
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_pricing_location()}
                ]
                
                response = self.pricing_client.get_products(
                    ServiceCode='AmazonRDS',
                    Filters=filters,
                    MaxResults=10
                )
                
                for price_item in response['PriceList']:
                    product = json.loads(price_item)
                    terms = product.get('terms', {})
                    on_demand_terms = terms.get('OnDemand', {})
                    
                    for term_key, term_data in on_demand_terms.items():
                        price_dimensions = term_data.get('priceDimensions', {})
                        for price_key, price_data in price_dimensions.items():
                            price_per_unit = price_data.get('pricePerUnit', {})
                            usd_price = price_per_unit.get('USD')
                            if usd_price:
                                io_price = float(usd_price)
                                break
                        break
                    break
                    
            except Exception as e:
                self.logger.warning(f"获取Aurora IO定价失败，使用默认值: {e}")
            
            self.logger.info(f"Aurora IO请求单价: ${io_price}/百万次请求")
            
            self.aurora_storage_pricing_cache = {
                'storage_gb_month': storage_price,
                'io_million_requests': io_price
            }
            
            self.logger.info("Aurora存储定价获取完成")
            return self.aurora_storage_pricing_cache
            
        except Exception as e:
            self.logger.error(f"获取Aurora存储定价失败: {e}")
            # 返回默认值
            self.aurora_storage_pricing_cache = {
                'storage_gb_month': 0.1,
                'io_million_requests': 0.2
            }
            return self.aurora_storage_pricing_cache
    def get_all_rds_mysql_instances(self) -> List[Dict]:
        """获取所有RDS MySQL实例"""
        if self.enable_optimization:
            return self._get_instances_optimized()
        else:
            return self._get_instances_original()

    def _get_instances_original(self) -> List[Dict]:
        """原始模式获取实例"""
        instances = []
        account_id = self.get_account_id()
        
        try:
            # 获取所有Aurora集群信息
            aurora_clusters = {}
            try:
                paginator = self.rds_client.get_paginator('describe_db_clusters')
                for page in paginator.paginate():
                    for cluster in page['DBClusters']:
                        if cluster.get('Engine') == 'aurora-mysql':
                            aurora_clusters[cluster['DBClusterIdentifier']] = cluster
            except Exception as e:
                self.logger.warning(f"获取Aurora集群信息失败: {e}")
            
            # 获取所有RDS实例
            all_db_instances = []
            paginator = self.rds_client.get_paginator('describe_db_instances')
            
            for page in paginator.paginate():
                for db_instance in page['DBInstances']:
                    if (db_instance.get('Engine') == 'mysql' and
                        db_instance.get('DBInstanceStatus') == 'available' and
                        db_instance.get('EngineVersion', '').startswith('8.0') and
                        self._should_process_instance(db_instance['DBInstanceClass'])):
                        all_db_instances.append(db_instance)
            
            # 处理每个实例
            for db_instance in all_db_instances:
                instance_data = self._process_single_instance_complete(db_instance, all_db_instances, aurora_clusters, account_id)
                if instance_data:
                    instances.append(instance_data)
            
            self.logger.info(f"找到 {len(instances)} 个符合条件的RDS MySQL实例")
            return instances
            
        except Exception as e:
            self.logger.error(f"获取RDS实例失败: {e}")
            return []

    def _get_instances_optimized(self) -> List[Dict]:
        """优化模式获取实例"""
        instances = []
        account_id = self.get_account_id()
        
        try:
            # 获取所有Aurora集群信息
            aurora_clusters = {}
            try:
                paginator = self.rds_client.get_paginator('describe_db_clusters')
                for page in paginator.paginate():
                    for cluster in page['DBClusters']:
                        if cluster.get('Engine') == 'aurora-mysql':
                            aurora_clusters[cluster['DBClusterIdentifier']] = cluster
            except Exception as e:
                self.logger.warning(f"获取Aurora集群信息失败: {e}")
            
            # 获取所有实例
            all_instances = []
            paginator = self.rds_client.get_paginator('describe_db_instances')
            
            for page in paginator.paginate():
                for db_instance in page['DBInstances']:
                    if (db_instance.get('Engine') == 'mysql' and
                        db_instance.get('DBInstanceStatus') == 'available' and
                        db_instance.get('EngineVersion', '').startswith('8.0') and
                        self._should_process_instance(db_instance['DBInstanceClass'])):
                        all_instances.append(db_instance)
            
            self.logger.info(f"开始并发处理 {len(all_instances)} 个实例，使用 {self.max_workers} 个线程")
            
            # 并发处理实例
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_instance = {
                    executor.submit(self._process_single_instance_complete, db_instance, all_instances, aurora_clusters, account_id): db_instance
                    for db_instance in all_instances
                }
                
                for future in as_completed(future_to_instance):
                    try:
                        result = future.result()
                        if result:
                            instances.append(result)
                    except Exception as e:
                        db_instance = future_to_instance[future]
                        self.logger.error(f"处理实例 {db_instance.get('DBInstanceIdentifier', 'unknown')} 失败: {e}")
            
            self.logger.info(f"并发处理完成，成功处理 {len(instances)} 个实例")
            return instances
            
        except Exception as e:
            self.logger.error(f"优化模式获取实例失败: {e}")
            return []

    def _process_single_instance_complete(self, db_instance: Dict, all_db_instances: List[Dict], 
                                        aurora_clusters: Dict, account_id: str) -> Optional[Dict]:
        """完整处理单个实例"""
        try:
            instance_id = db_instance['DBInstanceIdentifier']
            instance_class = db_instance['DBInstanceClass']
            engine_version = db_instance.get('EngineVersion', 'unknown')
            
            # 确定架构类型
            architecture = self._determine_architecture_with_taz(db_instance, all_db_instances, aurora_clusters)
            
            # 获取存储信息
            storage_info = self.get_storage_info(instance_id, db_instance)
            
            # 获取CloudWatch指标
            cloudwatch_metrics = self.get_cloudwatch_metrics(instance_id)
            
            # 获取source_db_instance_identifier - 与原始脚本逻辑一致
            source_db_identifier = db_instance.get('ReadReplicaSourceDBInstanceIdentifier', '')
            
            # 处理集群级Read Replica
            if not source_db_identifier and db_instance.get('ReadReplicaSourceDBClusterIdentifier'):
                # 集群级Read Replica，需要找到源集群的Writer节点
                source_cluster = db_instance['ReadReplicaSourceDBClusterIdentifier']
                try:
                    cluster_response = self.rds_client.describe_db_clusters(DBClusterIdentifier=source_cluster)
                    cluster = cluster_response['DBClusters'][0]
                    for member in cluster.get('DBClusterMembers', []):
                        if member.get('IsClusterWriter', False):
                            source_db_identifier = member['DBInstanceIdentifier']
                            break
                except Exception as e:
                    self.logger.warning(f"获取源集群Writer失败: {e}")
            
            # 对于TAZ集群，通过API获取集群信息来确定writer节点
            if architecture == 'taz_cluster':
                cluster_id = db_instance.get('DBClusterIdentifier')
                if cluster_id:
                    try:
                        response = self.rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)
                        cluster = response['DBClusters'][0]
                        members = cluster.get('DBClusterMembers', [])
                        
                        # 找到Writer节点
                        writer_instance = None
                        for member in members:
                            if member.get('IsClusterWriter'):
                                writer_instance = member['DBInstanceIdentifier']
                                break
                        
                        if writer_instance and writer_instance != instance_id:
                            # 当前实例是Reader，设置source为Writer
                            source_db_identifier = writer_instance
                            self.logger.info(f"TAZ集群Reader节点 {instance_id} 的source_db_instance_identifier设置为Writer节点: {writer_instance}")
                        else:
                            # 当前实例是Writer
                            source_db_identifier = 'N/A'
                            
                    except Exception as e:
                        self.logger.warning(f"获取TAZ集群Writer失败: {e}")
                        source_db_identifier = 'N/A'
            
            # 对于RDS MySQL Read Replica，保持原有逻辑
            elif not source_db_identifier:
                source_db_identifier = 'N/A'
            
            return {
                'db_instance_identifier': instance_id,
                'account_id': account_id,
                'region': self.region,
                'mysql_engine_version': engine_version,
                'architecture': architecture,
                'instance_class': instance_class,
                'source_db_instance_identifier': source_db_identifier,
                'read_replica_source_db_cluster_identifier': db_instance.get('ReadReplicaSourceDBClusterIdentifier', ''),
                **storage_info,
                **cloudwatch_metrics
            }
            
        except Exception as e:
            self.logger.error(f"处理实例失败: {e}")
            return None

    def _determine_architecture_with_taz(self, db_instance: Dict, all_instances: List[Dict], aurora_clusters: Dict) -> str:
        """
        优化的架构判断 - 仅通过API，正确识别TAZ和主从关系
        """
        
        # 1. Read Replica优先，基于自身的MultiAZ状态
        if db_instance.get('ReadReplicaSourceDBInstanceIdentifier'):
            return 'multi_az' if db_instance.get('MultiAZ', False) else 'read_replica'
        
        # 2. 集群架构判断
        cluster_id = db_instance.get('DBClusterIdentifier')
        if cluster_id and cluster_id != 'N/A':
            try:
                response = self.rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)
                cluster = response['DBClusters'][0]
                members = cluster.get('DBClusterMembers', [])
                
                # TAZ: 3成员 + 1Writer/2Reader + 跨3AZ
                if (len(members) == 3 and 
                    len([m for m in members if m.get('IsClusterWriter')]) == 1 and
                    self._is_cross_three_az(members, all_instances)):
                    self.logger.info(f"实例 {db_instance['DBInstanceIdentifier']} 属于TAZ集群")
                    return 'taz_cluster'
                    
                return 'multi_az' if cluster.get('MultiAZ') else 'single_az'
            except Exception as e:
                self.logger.warning(f"获取集群信息失败: {e}")
        
        # 3. 实例级MultiAZ
        return 'multi_az' if db_instance.get('MultiAZ') else 'single_az'

    def _is_cross_three_az(self, members: List[Dict], all_instances: List[Dict]) -> bool:
        """检查集群是否跨3个不同的可用区"""
        azs = set()
        for member in members:
            for instance in all_instances:
                if instance['DBInstanceIdentifier'] == member['DBInstanceIdentifier']:
                    if az := instance.get('AvailabilityZone'):
                        azs.add(az)
                    break
        return len(azs) == 3

    def _get_cluster_info(self, db_instance: Dict, aurora_clusters: Dict) -> Dict:
        """
        获取实例的集群信息
        
        Args:
            db_instance: RDS实例信息
            aurora_clusters: Aurora集群信息字典
            
        Returns:
            集群信息字典
        """
        cluster_identifier = db_instance.get('DBClusterIdentifier')
        db_identifier = db_instance['DBInstanceIdentifier']
        
        if cluster_identifier and cluster_identifier in aurora_clusters:
            cluster_info = aurora_clusters[cluster_identifier]
            members = cluster_info.get('DBClusterMembers', [])
            
            # 查找当前实例在集群中的角色
            for member in members:
                if member['DBInstanceIdentifier'] == db_identifier:
                    return {
                        'cluster_identifier': cluster_identifier,
                        'is_cluster_writer': member.get('IsClusterWriter', False),
                        'promotion_tier': member.get('PromotionTier', 0),
                        'member_count': len(members)
                    }
        
        return {
            'cluster_identifier': None,
            'is_cluster_writer': False,
            'promotion_tier': 0,
            'member_count': 0
        }
    def analyze_instances(self) -> List[Dict]:
        """主分析方法"""
        self.logger.info(f"开始分析 - 模式: {'优化' if self.enable_optimization else '标准'}")
        
        # 获取所有定价信息
        self.get_all_rds_mysql_pricing()
        self.get_all_aurora_mysql_pricing()
        self.get_all_storage_pricing()
        self.get_aurora_storage_pricing()
        
        # 获取实例信息
        instances = self.get_all_rds_mysql_instances()
        
        # TAZ集群的source关系已在_analyze_single_instance中通过API设置
        
        # 识别RDS MySQL集群
        rds_mysql_clusters = self.identify_rds_mysql_clusters(instances)
        self.logger.info(f"识别到 {len(rds_mysql_clusters)} 个RDS MySQL集群")
        
        # 设置RDS MySQL集群关系
        for instance in instances:
            instance_id = instance['db_instance_identifier']
            for primary, replicas in rds_mysql_clusters.items():
                replica_ids = [inst['db_instance_identifier'] if isinstance(inst, dict) else inst for inst in replicas]
                if instance_id in replica_ids and len(replica_ids) >= 2 and instance_id != primary:
                    instance['source_db_instance_identifier'] = primary
                    self.logger.info(f"RDS MySQL集群Reader节点 {instance_id} 的source_db_instance_identifier设置为Primary节点: {primary}")
        
        results = []
        cluster_aurora_storage_costs = self._calculate_cluster_aurora_storage_costs(instances)
        
        for instance in instances:
            self.logger.info(f"分析实例: {instance['db_instance_identifier']}")
            result = self._analyze_single_instance(instance, cluster_aurora_storage_costs, rds_mysql_clusters)
            if result:
                results.append(result)
        
        return results

    def identify_rds_mysql_clusters(self, instances: List[Dict]) -> Dict[str, List[str]]:
        """识别RDS MySQL集群"""
        clusters = {}
        
        for instance in instances:
            instance_id = instance['db_instance_identifier']
            source_id = instance.get('source_db_instance_identifier', '')
            
            if source_id:
                # 这是一个Read Replica
                if source_id not in clusters:
                    clusters[source_id] = [source_id]
                if instance_id not in clusters[source_id]:
                    clusters[source_id].append(instance_id)
                self.logger.info(f"实例 {instance_id} 是 {source_id} 的Read Replica")
        
        return clusters

    def _calculate_cluster_aurora_storage_costs(self, instances: List[Dict]) -> Dict[str, float]:
        """计算集群Aurora存储成本 - 简化逻辑"""
        cluster_costs = {}
        storage_price = self.aurora_storage_pricing_cache.get('storage_gb_month', 0.1)
        
        for instance in instances:
            instance_id = instance['db_instance_identifier']
            source_id = instance.get('source_db_instance_identifier', '')
            
            # 只有Primary/Writer节点计算存储成本
            if not source_id or source_id in ['N/A', '']:
                used_storage = instance.get('used_storage_gb', 0)
                if used_storage <= 0:
                    used_storage = instance.get('allocated_storage_gb', 0)
                
                storage_cost = used_storage * storage_price
                cluster_costs[instance_id] = round(storage_cost, 2)
                self.logger.info(f"Primary/Writer节点 {instance_id} Aurora存储成本: ${storage_cost:.2f} (存储: {used_storage}GB)")
            else:
                # Read Replica节点存储成本为0
                cluster_costs[instance_id] = 0
                self.logger.info(f"Read Replica节点 {instance_id} Aurora存储成本为0")
        
        return cluster_costs

    def _analyze_single_instance(self, instance: Dict, cluster_aurora_storage_costs: Dict, rds_mysql_clusters: Dict = None) -> Dict:
        """分析单个实例"""
        instance_id = instance['db_instance_identifier']
        instance_class = instance['instance_class']
        architecture = instance['architecture']
        
        # 获取RDS定价
        rds_hourly_rate = self.pricing_cache.get(instance_class, 0)
        rds_adjusted_rate = self._adjust_pricing_for_architecture(rds_hourly_rate, architecture)
        rds_instance_mrr = round(rds_adjusted_rate * 730, 2) if rds_adjusted_rate else 0
        
        # 计算存储成本
        storage_cost_info = self._calculate_storage_cost(instance, architecture)
        rds_total_mrr = rds_instance_mrr + storage_cost_info['total_storage_cost_per_month_usd']
        
        # 基础结果
        result = {
            'account_id': instance['account_id'],
            'region': instance['region'],
            'cluster_primary_instance': self._get_cluster_primary_instance(instance, rds_mysql_clusters),
            'mysql_engine_version': instance['mysql_engine_version'],
            'db_instance_identifier': instance_id,
            'source_db_instance_identifier': instance.get('source_db_instance_identifier', '') or '',
            'architecture': architecture,
            'rds_instance_class': instance_class,
            'rds_hourly_rate_usd': rds_adjusted_rate if rds_adjusted_rate else 'NA',
            'rds_instance_mrr_usd': rds_instance_mrr,
            'storage_type': instance.get('storage_type', ''),
            'allocated_storage_gb': instance.get('allocated_storage_gb', 0),
            'used_storage_gb': instance.get('used_storage_gb', 0),
            'iops': instance.get('iops', 0),
            'storage_throughput_mbps': instance.get('storage_throughput', 0),
            'iops-avg.1Hr/15-day period': instance.get('iops-avg.1Hr/15-day period', 0),
            'cpu-max.1Hr/15-day period': instance.get('cpu-max.1Hr/15-day period', 0),
            'cpu-min.1Hr/15-day period': instance.get('cpu-min.1Hr/15-day period', 0),
            'cpu-avg.1Hr/15-day period': instance.get('cpu-avg.1Hr/15-day period', 0),
            **storage_cost_info,
            'rds_total_mrr_usd': round(rds_total_mrr, 2),
            'aurora_allocate_storage_gb': cluster_aurora_storage_costs.get(instance_id, 0) / self.aurora_storage_pricing_cache.get('storage_gb_month', 0.1) if cluster_aurora_storage_costs.get(instance_id, 0) > 0 else 0,
            'aurora_storage_price_per_gb_month_usd': self.aurora_storage_pricing_cache.get('storage_gb_month', 0.1) if cluster_aurora_storage_costs.get(instance_id, 0) > 0 else 0,
            'aurora_total_storage_cost_per_month_usd': cluster_aurora_storage_costs.get(instance_id, 0),
            'aurora_io_price_per_million_requests_usd': self.aurora_storage_pricing_cache.get('io_million_requests', 0.2),
            'aurora_monthly_io_requests_total': int(instance.get('iops-avg.1Hr/15-day period', 0) * 730),
            'aurora_io_cost_mrr_usd': round((instance.get('iops-avg.1Hr/15-day period', 0) * 730 / 1000000) * self.aurora_storage_pricing_cache.get('io_million_requests', 0.2), 2)
        }
        
        # Aurora分析
        for generation in self.aurora_generations:
            aurora_result = self._analyze_aurora_conversion(instance, generation, cluster_aurora_storage_costs)
            result.update(aurora_result)
        
        # RDS替换分析
        for generation in self.rds_replacement_generations:
            rds_replacement_result = self._analyze_rds_replacement(instance, generation, storage_cost_info)
            result.update(rds_replacement_result)
        
        return result

    def _get_cluster_primary_instance(self, instance: Dict, rds_clusters: Dict = None) -> str:
        """获取集群主实例（与原始脚本逻辑一致）"""
        instance_id = instance['db_instance_identifier']
        
        # 检查是否是RDS MySQL集群的primary节点
        if rds_clusters and instance_id in rds_clusters:
            return instance_id
        
        # 检查是否是RDS MySQL集群的read replica
        if rds_clusters:
            for primary_id, cluster_instances in rds_clusters.items():
                cluster_instance_ids = [inst['db_instance_identifier'] if isinstance(inst, dict) else inst for inst in cluster_instances]
                if instance_id in cluster_instance_ids and instance_id != primary_id:
                    return primary_id
        
        # 检查TAZ集群 - 通过API获取集群信息
        architecture = instance.get('architecture', '')
        if architecture == 'taz_cluster':
            source_db = instance.get('source_db_instance_identifier')
            if source_db and source_db != 'N/A' and source_db.strip():
                return source_db
            else:
                return instance_id  # Writer节点
        
        # 检查source_db_instance_identifier
        source_db = instance.get('source_db_instance_identifier')
        if source_db and source_db != 'N/A' and source_db.strip():
            return source_db
        
        # 默认返回实例自身
        return instance_id

    def _adjust_pricing_for_architecture(self, hourly_rate: float, architecture: str) -> float:
        """根据架构调整定价（与原始脚本一致）"""
        if architecture in ['multi_az']:
            self.logger.debug(f"实例是{architecture}，RDS成本乘以2")
            return hourly_rate * 2
        elif architecture == 'taz_cluster':
            self.logger.debug(f"实例是TAZ集群成员，使用单价")
            return hourly_rate
        return hourly_rate

    def _calculate_storage_cost(self, instance: Dict, architecture: str) -> Dict:
        """计算存储成本"""
        storage_type = instance.get('storage_type', 'gp2')
        allocated_gb = instance.get('allocated_storage_gb', 0)
        iops = instance.get('iops', 0)
        
        # 获取存储定价
        storage_pricing = self.storage_pricing_cache.get(storage_type, {})
        storage_price_per_gb = storage_pricing.get('storage_gb_month', 0.115)
        iops_price_per_iops = storage_pricing.get('iops_month', 0)
        
        # 计算基础成本
        storage_cost = allocated_gb * storage_price_per_gb
        iops_cost = iops * iops_price_per_iops
        total_cost = storage_cost + iops_cost
        
        # Multi-AZ架构存储成本乘以2
        if architecture == 'multi_az':
            storage_cost *= 2
            iops_cost *= 2
            total_cost *= 2
        
        return {
            'storage_price_per_gb_month_usd': storage_price_per_gb,
            'iops_price_per_iops_month_usd': iops_price_per_iops,
            'storage_cost_per_month_usd': round(storage_cost, 2),
            'iops_cost_per_month_usd': round(iops_cost, 2),
            'total_storage_cost_per_month_usd': round(total_cost, 2)
        }

    def _analyze_aurora_conversion(self, instance: Dict, generation: str, cluster_aurora_storage_costs: Dict) -> Dict:
        """分析Aurora转换"""
        instance_id = instance['db_instance_identifier']
        instance_class = instance['instance_class']
        architecture = instance['architecture']
        
        # 映射到Aurora实例
        aurora_instance_class = self._map_to_aurora_instance(instance_class, generation)
        
        # 获取Aurora定价
        aurora_hourly_rate = self.aurora_pricing_cache.get(aurora_instance_class, 0)
        
        # Aurora实例成本不需要乘以2（即使是Multi-AZ）
        aurora_instance_mrr = round(aurora_hourly_rate * 730, 2) if aurora_hourly_rate else 0
        
        # 获取存储成本和IO成本（引用统一的IO成本字段）
        storage_cost = cluster_aurora_storage_costs.get(instance_id, 0)
        io_cost = instance.get('aurora_io_cost_mrr_usd', 0)
        
        # 总成本
        total_cost = aurora_instance_mrr + storage_cost + io_cost
        
        # Optimized模式计算（与原始脚本一致）
        optimized_total_cost = aurora_instance_mrr * 1.3 + storage_cost * 2.25 + io_cost if aurora_instance_mrr else 0
        
        return {
            f'aurora_{generation}_instance_class': aurora_instance_class,
            f'aurora_{generation}_hourly_rate_usd': aurora_hourly_rate if aurora_hourly_rate else 'NA',
            f'aurora_{generation}_standard_instance_mrr_usd': aurora_instance_mrr,
            f'aurora_{generation}_standard_total_mrr_usd': round(total_cost, 2),
            f'aurora_{generation}_optimized_instance_mrr_usd': round(aurora_instance_mrr * 1.3, 2) if aurora_instance_mrr else 'NA',
            f'aurora_{generation}_optimized_total_mrr_usd': round(optimized_total_cost, 2)
        }

    def _analyze_rds_replacement(self, instance: Dict, generation: str, storage_cost_info: Dict) -> Dict:
        """分析RDS替换"""
        instance_class = instance['instance_class']
        architecture = instance['architecture']
        
        # 映射到RDS替换实例
        replacement_instance_class = self._map_to_rds_replacement_instance(instance_class, generation)
        
        if not replacement_instance_class:
            return {
                f'rds_replacement_{generation}_instance_class': 'NA',
                f'rds_replacement_{generation}_hourly_rate_usd': 'NA',
                f'rds_replacement_{generation}_instance_mrr_usd': 'NA',
                f'rds_replacement_{generation}_storage_mrr_usd': 'NA',
                f'rds_replacement_{generation}_total_mrr_usd': 'NA'
            }
        
        # 获取RDS替换定价
        replacement_hourly_rate = self.pricing_cache.get(replacement_instance_class, 0)
        replacement_adjusted_rate = self._adjust_pricing_for_architecture(replacement_hourly_rate, architecture)
        replacement_instance_mrr = round(replacement_adjusted_rate * 730, 2) if replacement_adjusted_rate else 0
        
        # 存储成本
        storage_mrr = storage_cost_info['total_storage_cost_per_month_usd']
        total_mrr = replacement_instance_mrr + storage_mrr
        
        return {
            f'rds_replacement_{generation}_instance_class': replacement_instance_class,
            f'rds_replacement_{generation}_hourly_rate_usd': replacement_adjusted_rate if replacement_adjusted_rate else 'NA',
            f'rds_replacement_{generation}_instance_mrr_usd': replacement_instance_mrr,
            f'rds_replacement_{generation}_storage_mrr_usd': storage_mrr,
            f'rds_replacement_{generation}_total_mrr_usd': round(total_mrr, 2)
        }

    def _map_to_aurora_instance(self, rds_instance_class: str, aurora_generation: str) -> str:
        """映射到Aurora实例"""
        instance_type = rds_instance_class.replace('db.', '')
        parts = instance_type.split('.')
        
        if len(parts) >= 2:
            size = parts[1]
            return f"db.{aurora_generation}.{size}"
        else:
            return f"db.{aurora_generation}.large"

    def _map_to_rds_replacement_instance(self, rds_instance_class: str, replacement_generation: str) -> Optional[str]:
        """映射到RDS替换实例"""
        instance_type = rds_instance_class.replace('db.', '')
        parts = instance_type.split('.')
        
        if len(parts) >= 2:
            instance_family = parts[0]
            size = parts[1]
            
            # M系列只能映射到m7g/m8g
            if instance_family.startswith('m') and replacement_generation in ['m7g', 'm8g']:
                return f"db.{replacement_generation}.{size}"
            # R系列只能映射到r7g/r8g
            elif instance_family.startswith('r') and replacement_generation in ['r7g', 'r8g']:
                return f"db.{replacement_generation}.{size}"
            # C系列映射到m7g/m8g
            elif instance_family.startswith('c') and replacement_generation in ['m7g', 'm8g']:
                return f"db.{replacement_generation}.{size}"
        
        return None
    def export_to_excel(self, results: List[Dict], output_file: str) -> None:
        """导出到Excel（与原始脚本完全一致，包含两个sheet）"""
        if not results:
            self.logger.warning("没有数据可导出")
            return
        
        try:
            # 生成集群汇总数据
            cluster_summary = self.generate_cluster_summary(results)
            
            # 创建Excel writer
            excel_file = output_file.replace('.csv', '.xlsx')
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                # 详细数据sheet
                df_details = pd.DataFrame(results)
                df_details.to_excel(writer, sheet_name='详细数据', index=False)
                
                # 集群成本汇总sheet
                if cluster_summary:
                    df_cluster_summary = pd.DataFrame(cluster_summary)
                    df_cluster_summary.to_excel(writer, sheet_name='cluster成本汇总', index=False)
            
            self.logger.info(f"Excel结果已导出到: {excel_file}")
            self.logger.info(f"包含 {len(results)} 条详细记录, {len(cluster_summary)} 个集群汇总")
            
        except Exception as e:
            self.logger.error(f"导出Excel失败: {e}")

    def generate_cluster_summary(self, results: List[Dict]) -> List[Dict]:
        """生成集群成本汇总（与原始脚本完全一致）"""
        clusters = {}
        
        for result in results:
            # 确定集群标识：优先使用source_db_instance_identifier的Primary节点，否则使用实例自身
            source_db = result.get('source_db_instance_identifier', 'N/A')
            if source_db != 'N/A' and source_db is not None and source_db.strip():
                cluster_key = source_db  # 使用Primary节点作为集群标识
            else:
                cluster_key = result.get('db_instance_identifier', 'unknown')  # Primary节点使用自身
            
            rds_instance_class = result.get('rds_instance_class', '')
            
            # 判断实例系列
            instance_family = rds_instance_class.replace('db.', '').split('.')[0] if rds_instance_class else ''
            series_type = 'R系列' if instance_family.startswith('r') else 'M/C系列'
            
            if cluster_key not in clusters:
                base_fields = {
                    'account_id': result.get('account_id'),
                    'region': result.get('region'),
                    'cluster_primary_instance': cluster_key,
                    'instance_count': 0,
                    'rds_total_mrr_usd': 0,
                    'aurora_r7g_standard_total_mrr_usd': 0,
                    'aurora_r8g_standard_total_mrr_usd': 0,
                    'aurora_r7g_optimized_total_mrr_usd': 0,
                    'aurora_r8g_optimized_total_mrr_usd': 0,
                    'replacement_r7g_m7g_total_mrr_usd': 0,
                    'replacement_r8g_m8g_total_mrr_usd': 0
                }
                
                clusters[cluster_key] = base_fields
            
            summary = clusters[cluster_key]
            summary['instance_count'] += 1
            
            # 汇总所有成本字段
            cost_fields = ['rds_total_mrr_usd', 'aurora_r7g_standard_total_mrr_usd', 'aurora_r8g_standard_total_mrr_usd',
                          'aurora_r7g_optimized_total_mrr_usd', 'aurora_r8g_optimized_total_mrr_usd']
            
            for field in cost_fields:
                value = result.get(field, 0)
                if isinstance(value, (int, float)):
                    summary[field] += value
            
            # 合并RDS替换成本字段
            r7g_m7g_cost = (self._safe_float(result.get('rds_replacement_r7g_total_mrr_usd', 0)) + 
                           self._safe_float(result.get('rds_replacement_m7g_total_mrr_usd', 0)))
            r8g_m8g_cost = (self._safe_float(result.get('rds_replacement_r8g_total_mrr_usd', 0)) + 
                           self._safe_float(result.get('rds_replacement_m8g_total_mrr_usd', 0)))
            
            summary['replacement_r7g_m7g_total_mrr_usd'] += r7g_m7g_cost
            summary['replacement_r8g_m8g_total_mrr_usd'] += r8g_m8g_cost
        
        # 转换为列表并四舍五入
        summary_list = []
        for summary in clusters.values():
            for key, value in summary.items():
                if key.endswith('_usd') and isinstance(value, (int, float)):
                    summary[key] = round(value, 2)
            summary_list.append(summary)
        
        return summary_list

    def _safe_float(self, value) -> float:
        """安全转换为float"""
        if value == 'NA' or value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def run_analysis(self, output_file: str = None) -> None:
        """运行完整分析"""
        start_time = time.time()
        
        try:
            results = self.analyze_instances()
            
            if not output_file:
                mode = "optimized" if self.enable_optimization else "standard"
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"rds_aurora_analysis_{mode}_{self.region}_{timestamp}.xlsx"
            
            self.export_to_excel(results, output_file)
            
            end_time = time.time()
            self.logger.info(f"分析完成，总耗时: {end_time - start_time:.2f}秒")
            
            # 打印总结信息
            self._print_summary(results, output_file)
            
        except Exception as e:
            self.logger.error(f"分析失败: {e}")
            raise

    def _print_summary(self, results: List[Dict], output_file: str) -> None:
        """打印分析总结"""
        print(f"分析完成！共处理 {len(results)} 个实例，结果已保存到 {output_file}")
        print("RDS成本包含：")
        print("- 实例MRR：RDS实例的月度费用（不含存储）")
        print("- 存储MRR：RDS存储的月度费用")
        print("- 总MRR：实例MRR + 存储MRR")
        print("Aurora转换成本包含：")
        print("- 实例MRR：Aurora实例的月度费用（不含存储）")
        print("- 存储MRR：Aurora存储的月度费用（基于Primary节点的used_storage_gb）")
        print("- 总MRR：实例MRR + 存储MRR")
        print("RDS替换成本包含：")
        print("- 实例MRR：RDS替换实例的月度费用（不含存储）")
        print("- 存储MRR：RDS替换存储的月度费用")
        print("- 总MRR：实例MRR + 存储MRR")


def main():
    parser = argparse.ArgumentParser(description='完整统一版RDS Aurora多代定价分析工具')
    parser.add_argument('region', help='AWS区域名称')
    parser.add_argument('-o', '--output', help='输出文件名')
    parser.add_argument('--optimize', action='store_true', help='启用优化模式（并发处理）')
    parser.add_argument('-w', '--workers', type=int, default=10, help='并发线程数 (默认: 10)')
    parser.add_argument('-b', '--batch-size', type=int, default=50, help='批处理大小 (默认: 50)')
    
    args = parser.parse_args()
    
    # 限制最大线程数
    max_workers = min(args.workers, 15)
    
    try:
        analyzer = CompleteUnifiedRDSAuroraAnalyzer(
            region=args.region,
            enable_optimization=args.optimize,
            max_workers=max_workers,
            batch_size=args.batch_size
        )
        
        analyzer.run_analysis(args.output)
        
    except KeyboardInterrupt:
        print("\n分析被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"分析失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
