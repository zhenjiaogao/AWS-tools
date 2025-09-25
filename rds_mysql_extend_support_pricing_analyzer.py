#!/usr/bin/env python3
"""
RDS MySQL实例定价分析脚本
获取指定region的RDS MySQL实例信息，计算RI和Extended Support费用
"""

import boto3
import json
import csv
import argparse
from datetime import datetime
from typing import Dict, List, Any
import sys

class RDSPricingAnalyzer:
    def __init__(self, region: str):
        self.region = region
        self.rds_client = boto3.client('rds', region_name=region)
        self.pricing_client = boto3.client('pricing', region_name='us-east-1')  # Pricing API只在us-east-1可用
        self.sts_client = boto3.client('sts', region_name=region)
        
        # CPU核心数缓存，避免重复API调用
        self.cpu_cores_cache = {}

    def get_all_instance_cpu_cores(self, instance_classes: List[str]) -> Dict[str, int]:
        """批量获取多个实例类型的CPU核心数，提高效率"""
        cpu_cores_mapping = {}
        
        # 首先尝试从Pricing API批量获取
        try:
            print(f"正在批量获取 {len(instance_classes)} 个实例类型的CPU核心数...")
            
            # 构建过滤器，不指定具体实例类型
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'serviceCode', 'Value': 'AmazonRDS'},
                {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_location_name()},
            ]
            
            # 使用分页获取所有产品
            paginator = self.pricing_client.get_paginator('get_products')
            page_iterator = paginator.paginate(
                ServiceCode='AmazonRDS',
                Filters=filters,
                MaxResults=100
            )
            
            for page in page_iterator:
                for product_str in page.get('PriceList', []):
                    product = json.loads(product_str)
                    attributes = product.get('product', {}).get('attributes', {})
                    
                    instance_type = attributes.get('instanceType', '')
                    if instance_type in instance_classes and instance_type not in cpu_cores_mapping:
                        # 尝试从不同的属性字段获取CPU信息
                        cpu_info = None
                        for cpu_field in ['vcpu', 'vCPU', 'processorFeatures', 'instanceFamily']:
                            if cpu_field in attributes:
                                cpu_info = attributes[cpu_field]
                                break
                        
                        if cpu_info:
                            cpu_cores = self._parse_cpu_cores(cpu_info, instance_type)
                            if cpu_cores > 0:
                                cpu_cores_mapping[instance_type] = cpu_cores
                                self.cpu_cores_cache[instance_type] = cpu_cores
            
            print(f"从Pricing API成功获取 {len(cpu_cores_mapping)} 个实例类型的CPU核心数")
            
        except Exception as e:
            print(f"批量获取CPU核心数失败: {e}")
        
        # 对于未能从API获取的实例类型，使用推断方法
        for instance_class in instance_classes:
            if instance_class not in cpu_cores_mapping:
                cpu_cores = self._infer_cpu_cores_from_instance_name(instance_class)
                if cpu_cores > 0:
                    cpu_cores_mapping[instance_class] = cpu_cores
                    self.cpu_cores_cache[instance_class] = cpu_cores
                else:
                    # 使用默认值
                    cpu_cores_mapping[instance_class] = 2
                    self.cpu_cores_cache[instance_class] = 2
                    print(f"警告: 无法获取实例类型 {instance_class} 的CPU核心数，使用默认值 2")
        
        print(f"总共获取 {len(cpu_cores_mapping)} 个实例类型的CPU核心数")
        return cpu_cores_mapping

    def get_instance_cpu_cores(self, instance_class: str) -> int:
        """通过AWS Pricing API获取实例类型的CPU核心数"""
        # 检查缓存
        if instance_class in self.cpu_cores_cache:
            return self.cpu_cores_cache[instance_class]
        
        try:
            print(f"正在获取实例类型 {instance_class} 的CPU核心数...")
            
            # 构建过滤器查询实例规格信息
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'serviceCode', 'Value': 'AmazonRDS'},
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_class},
                {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_location_name()},
            ]
            
            response = self.pricing_client.get_products(
                ServiceCode='AmazonRDS',
                Filters=filters,
                MaxResults=10
            )
            
            # 解析产品信息获取CPU核心数
            for product_str in response.get('PriceList', []):
                product = json.loads(product_str)
                attributes = product.get('product', {}).get('attributes', {})
                
                # 尝试从不同的属性字段获取CPU信息
                cpu_info = None
                for cpu_field in ['vcpu', 'vCPU', 'processorFeatures', 'instanceFamily']:
                    if cpu_field in attributes:
                        cpu_info = attributes[cpu_field]
                        break
                
                if cpu_info:
                    # 解析CPU核心数
                    cpu_cores = self._parse_cpu_cores(cpu_info, instance_class)
                    if cpu_cores > 0:
                        self.cpu_cores_cache[instance_class] = cpu_cores
                        print(f"实例类型 {instance_class} 的CPU核心数: {cpu_cores}")
                        return cpu_cores
            
            # 如果无法从Pricing API获取，尝试从实例类型名称推断
            cpu_cores = self._infer_cpu_cores_from_instance_name(instance_class)
            if cpu_cores > 0:
                self.cpu_cores_cache[instance_class] = cpu_cores
                print(f"从实例名称推断 {instance_class} 的CPU核心数: {cpu_cores}")
                return cpu_cores
            
            # 如果仍然无法获取，使用默认值并记录警告
            print(f"警告: 无法获取实例类型 {instance_class} 的CPU核心数，使用默认值 2")
            self.cpu_cores_cache[instance_class] = 2
            return 2
            
        except Exception as e:
            print(f"获取实例类型 {instance_class} 的CPU核心数失败: {e}")
            # 尝试从实例类型名称推断
            cpu_cores = self._infer_cpu_cores_from_instance_name(instance_class)
            if cpu_cores > 0:
                self.cpu_cores_cache[instance_class] = cpu_cores
                return cpu_cores
            
            # 使用默认值
            self.cpu_cores_cache[instance_class] = 2
            return 2

    def _parse_cpu_cores(self, cpu_info: str, instance_class: str) -> int:
        """解析CPU信息字符串，提取核心数"""
        try:
            # 移除常见的单位和描述词
            cpu_str = str(cpu_info).lower().strip()
            
            # 尝试提取数字
            import re
            numbers = re.findall(r'\d+', cpu_str)
            
            if numbers:
                # 通常第一个数字就是CPU核心数
                cpu_cores = int(numbers[0])
                
                # 验证合理性（RDS实例通常在1-192核心之间）
                if 1 <= cpu_cores <= 192:
                    return cpu_cores
            
            return 0
            
        except Exception as e:
            print(f"解析CPU信息失败 {cpu_info}: {e}")
            return 0

    def _infer_cpu_cores_from_instance_name(self, instance_class: str) -> int:
        """从实例类型名称推断CPU核心数"""
        try:
            # RDS实例命名规律: db.{family}.{size}
            # 例如: db.r5.xlarge, db.m5.2xlarge, db.t3.micro
            
            parts = instance_class.split('.')
            if len(parts) != 3:
                return 0
            
            family = parts[1]  # r5, m5, t3等
            size = parts[2]    # micro, small, medium, large, xlarge, 2xlarge等
            
            # 基于实例大小的基础核心数映射
            size_mapping = {
                'nano': 1,
                'micro': 1,
                'small': 1,
                'medium': 1,
                'large': 2,
                'xlarge': 4,
                '2xlarge': 8,
                '3xlarge': 12,
                '4xlarge': 16,
                '6xlarge': 24,
                '8xlarge': 32,
                '9xlarge': 36,
                '10xlarge': 40,
                '12xlarge': 48,
                '16xlarge': 64,
                '18xlarge': 72,
                '24xlarge': 96,
                '32xlarge': 128,
                '48xlarge': 192,
            }
            
            # 特殊处理某些实例族
            if family.startswith('t'):  # t3, t4g等突发性能实例
                # 突发性能实例通常有较少的CPU核心
                if size in ['micro', 'small', 'medium', 'large']:
                    return 2
                elif size == 'xlarge':
                    return 4
                elif size == '2xlarge':
                    return 8
            
            # 使用通用映射
            return size_mapping.get(size, 2)
            
        except Exception as e:
            print(f"从实例名称推断CPU核心数失败 {instance_class}: {e}")
            return 0

    def get_account_id(self) -> str:
        """获取当前AWS账户ID"""
        try:
            response = self.sts_client.get_caller_identity()
            return response['Account']
        except Exception as e:
            print(f"获取账户ID失败: {e}")
            return "unknown"

    def get_rds_instances(self) -> List[Dict[str, Any]]:
        """获取指定region下的所有RDS MySQL实例"""
        instances = []
        try:
            paginator = self.rds_client.get_paginator('describe_db_instances')
            
            for page in paginator.paginate():
                for db_instance in page['DBInstances']:
                    if db_instance['Engine'].lower() == 'mysql':
                        # 判断架构类型
                        architecture = self._determine_architecture(db_instance)
                        
                        instance_info = {
                            'db_instance_identifier': db_instance['DBInstanceIdentifier'],
                            'account_id': self.get_account_id(),
                            'region': self.region,
                            'engine_version': db_instance['EngineVersion'],
                            'architecture': architecture,
                            'instance_class': db_instance['DBInstanceClass'],
                            'cpu_cores': 0,  # 将在后续批量获取中设置
                            'multi_az': db_instance.get('MultiAZ', False),
                            'read_replica_source': db_instance.get('ReadReplicaSourceDBInstanceIdentifier', ''),
                            'availability_zone': db_instance.get('AvailabilityZone', ''),
                        }
                        instances.append(instance_info)
                        
        except Exception as e:
            print(f"获取RDS实例失败: {e}")
            
        return instances

    def _determine_architecture(self, db_instance: Dict) -> str:
        """判断RDS实例的架构类型"""
        if db_instance.get('ReadReplicaSourceDBInstanceIdentifier'):
            return 'read_replica'
        elif db_instance.get('MultiAZ', False):
            return 'MAZ'
        else:
            return 'Non-MAZ'

    def get_all_ri_pricing(self) -> Dict[str, float]:
        """一次性获取该region下所有MySQL实例类型的1年RI无预付价格"""
        ri_pricing_cache = {}
        
        try:
            print("正在获取所有MySQL实例类型的RI定价...")
            
            # 构建过滤器，不指定具体实例类型
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'serviceCode', 'Value': 'AmazonRDS'},
                {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'},

                #{'Type': 'TERM_MATCH', 'Field': 'termType', 'Value': 'Reserved'},
                #{'Type': 'TERM_MATCH', 'Field': 'leaseContractLength', 'Value': '1yr'},
                #{'Type': 'TERM_MATCH', 'Field': 'purchaseOption', 'Value': 'No Upfront'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_location_name()},
                {'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Single-AZ'},
            ]
            
            # 分页获取产品
            paginator = self.pricing_client.get_paginator('get_products')
            page_iterator = paginator.paginate(
                ServiceCode='AmazonRDS',
                Filters=filters,
                MaxResults=100
            )
            
            for page in page_iterator:
                for product_str in page.get('PriceList', []):
                    product = json.loads(product_str)
                    attributes = product.get('product', {}).get('attributes', {})
                    instance_type = attributes.get('instanceType', '')
                    
                    if not instance_type:
                        continue  # 跳过无实例类型的记录
                    
                    # 解析预留实例条款（此时过滤器已确保是1年无预付，二次验证更严谨）
                    reserved_terms = product.get('terms', {}).get('Reserved', {})
                    for term in reserved_terms.values():
                        term_attrs = term.get('termAttributes', {})
                        # 二次验证租期和预付方式（防止过滤器失效）
                        if (term_attrs.get('LeaseContractLength') == '1yr' and 
                            term_attrs.get('PurchaseOption') == 'No Upfront'):
                            
                            # 提取每小时价格
                            for price_dim in term.get('priceDimensions', {}).values():
                                if price_dim.get('unit') == 'Hrs':
                                    price = float(price_dim.get('pricePerUnit', {}).get('USD', 0))
                                    if price > 0:
                                        ri_pricing_cache[instance_type] = price  # 自动去重（后出现的会覆盖，不影响）
                                    break  # 找到价格后跳出当前term循环
                    # 无需额外break，因过滤器已确保每个product对应唯一实例类型的目标价格
                
            print(f"成功获取 {len(ri_pricing_cache)} 个实例类型的1年RI无预付定价")
            
        except Exception as e:
            print(f"获取RI定价失败: {e}")

        #print("1年RI无预付价格列表:", ri_pricing_cache)
        return ri_pricing_cache

    def get_extended_support_pricing(self, cpu_cores: int) -> Dict[str, float]:
        """获取Extended Support每核心每小时的定价（基于实际API数据）"""
        
        # 基于实际API查询结果的定价数据
        if self.region == 'us-east-1':
            return {
                'year1_per_core_per_hour': 0.1000,  # MySQL Yr1-Yr2: $0.1/vCPU/小时
                'year2_per_core_per_hour': 0.1000,  # MySQL Yr1-Yr2: $0.1/vCPU/小时
                'year3_per_core_per_hour': 0.2000,  # MySQL Yr3: $0.2/vCPU/小时
            }
        
        # 其他区域的定价映射（基于实际API数据）
        region_pricing_map = {
            'us-east-2': {'year1': 0.1000, 'year2': 0.1000, 'year3': 0.2000},  # Ohio
            'us-west-1': {'year1': 0.1120, 'year2': 0.1120, 'year3': 0.2240},  # N. California
            'us-west-2': {'year1': 0.1000, 'year2': 0.1000, 'year3': 0.2000},  # Oregon
            'eu-west-1': {'year1': 0.1120, 'year2': 0.1120, 'year3': 0.2240},  # Ireland
            'eu-central-1': {'year1': 0.1220, 'year2': 0.1220, 'year3': 0.2440},  # Frankfurt
            'ap-southeast-1': {'year1': 0.1200, 'year2': 0.1200, 'year3': 0.2400},  # Singapore
            'ap-northeast-1': {'year1': 0.1200, 'year2': 0.1200, 'year3': 0.2400},  # Tokyo
        }
        
        if self.region in region_pricing_map:
            pricing = region_pricing_map[self.region]
            return {
                'year1_per_core_per_hour': pricing['year1'],
                'year2_per_core_per_hour': pricing['year2'],
                'year3_per_core_per_hour': pricing['year3'],
            }
        
        # 如果区域不在映射中，使用美东-1的定价作为默认值
        print(f"警告: 区域 {self.region} 的Extended Support定价未知，使用美东-1定价")
        return {
            'year1_per_core_per_hour': 0.1000,
            'year2_per_core_per_hour': 0.1000,
            'year3_per_core_per_hour': 0.2000,
        }
        """通过AWS Pricing API获取Extended Support每核心每小时的定价（第1、2、3年）"""
        try:
            location = self._get_location_name()
            
            pricing_data = {
                'year1_per_core_per_hour': 0.0,
                'year2_per_core_per_hour': 0.0,
                'year3_per_core_per_hour': 0.0,
            }
            
            # 尝试多种过滤器组合来查找Extended Support定价
            filter_combinations = [
                # 方法1: 直接查找ExtendedSupport
                [
                    {'Type': 'TERM_MATCH', 'Field': 'serviceCode', 'Value': 'AmazonRDS'},
                    {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'},
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': location},
                    {'Type': 'TERM_MATCH', 'Field': 'usagetype', 'Value': 'ExtendedSupport'},
                ],
                # 方法2: 查找包含extended的使用类型
                [
                    {'Type': 'TERM_MATCH', 'Field': 'serviceCode', 'Value': 'AmazonRDS'},
                    {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'},
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': location},
                ],
                # 方法3: 查找RDS相关的所有Extended Support
                [
                    {'Type': 'TERM_MATCH', 'Field': 'serviceCode', 'Value': 'AmazonRDS'},
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': location},
                ],
            ]
            
            for filters in filter_combinations:
                try:
                    response = self.pricing_client.get_products(
                        ServiceCode='AmazonRDS',
                        Filters=filters,
                        MaxResults=100
                    )
                    
                    found_pricing = self._parse_extended_support_pricing(response)
                    #print("##################")
                    #print(found_pricing)

                    if any(price > 0 for price in found_pricing.values()):
                        pricing_data.update(found_pricing)
                        break
                        
                except Exception as e:
                    print(f"查询Extended Support定价失败 (过滤器组合): {e}")
                    continue
            
            # 如果仍然没有找到定价，尝试查询MySQL Extended Support的通用定价
            #if all(price == 0.0 for price in pricing_data.values()):
            #    pricing_data = self._query_mysql_extended_support_generic()
            
            # 如果仍然没有找到定价，返回NA
            if all(price == 0.0 for price in pricing_data.values()):
                pricing_data = {
                    'year1_per_core_per_hour': 'NA',
                    'year2_per_core_per_hour': 'NA',
                    'year3_per_core_per_hour': 'NA',
                }
                print(f"警告: 无法从API获取Extended Support定价，返回NA")
            
            return pricing_data
            
        except Exception as e:
            print(f"获取Extended Support定价失败: {e}")
            return {
                'year1_per_core_per_hour': 'NA',
                'year2_per_core_per_hour': 'NA',
                'year3_per_core_per_hour': 'NA',
            }
    
    def _parse_extended_support_pricing(self, response: Dict) -> Dict[str, float]:
        """解析Extended Support定价响应"""
        pricing_data = {
            'year1_per_core_per_hour': 0.0,
            'year2_per_core_per_hour': 0.0,
            'year3_per_core_per_hour': 0.0,
        }
        
        for product_str in response.get('PriceList', []):
            product = json.loads(product_str)
            
            # 检查产品属性
            attributes = product.get('product', {}).get('attributes', {})
            usage_type = attributes.get('usagetype', '').lower()
            product_family = attributes.get('productFamily', '').lower()
            
            # 检查是否为Extended Support相关产品
            if not ('extended' in usage_type or 'extended' in product_family):
                continue
            
            # 解析定价信息
            terms = product.get('terms', {}).get('OnDemand', {})
            
            for term_key, term_data in terms.items():
                price_dimensions = term_data.get('priceDimensions', {})
                
                for price_key, price_data in price_dimensions.items():
                    price_per_unit = price_data.get('pricePerUnit', {}).get('USD', '0')
                    description = price_data.get('description', '').lower()
                    unit = price_data.get('unit', '').lower()
                    
                    if price_per_unit and price_per_unit != '0':
                        price_value = float(price_per_unit)
                        
                        # 根据描述和单位判断是第几年的定价
                        if 'core' in unit or 'vcpu' in unit:
                            if any(keyword in description for keyword in ['year 1', 'first year', '1st year']):
                                pricing_data['year1_per_core_per_hour'] = price_value
                                pricing_data['year2_per_core_per_hour'] = price_value
                                pricing_data['year3_per_core_per_hour'] = price_value*2
        
        return pricing_data
    
    def _query_mysql_extended_support_generic(self) -> Dict[str, float]:
        """查询MySQL Extended Support的通用定价"""
        try:
            # 查询所有MySQL Extended Support相关的产品
            response = self.pricing_client.get_products(
                ServiceCode='AmazonRDS',
                Filters=[
                    {'Type': 'TERM_MATCH', 'Field': 'serviceCode', 'Value': 'AmazonRDS'},
                    {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'},
                ],
                MaxResults=500
            )
            
            return self._parse_extended_support_pricing(response)
            
        except Exception as e:
            print(f"查询MySQL Extended Support通用定价失败: {e}")
            return {
                'year1_per_core_per_hour': 'NA',
                'year2_per_core_per_hour': 'NA',
                'year3_per_core_per_hour': 'NA',
            }
    
    def _get_location_name(self) -> str:
        """将AWS region转换为Pricing API使用的location名称"""
        region_mapping = {
            'us-east-1': 'US East (N. Virginia)',
            'us-east-2': 'US East (Ohio)',
            'us-west-1': 'US West (N. California)',
            'us-west-2': 'US West (Oregon)',
            'eu-west-1': 'Europe (Ireland)',
            'eu-west-2': 'Europe (London)',
            'eu-west-3': 'Europe (Paris)',
            'eu-central-1': 'Europe (Frankfurt)',
            'ap-southeast-1': 'Asia Pacific (Singapore)',
            'ap-southeast-2': 'Asia Pacific (Sydney)',
            'ap-northeast-1': 'Asia Pacific (Tokyo)',
            'ap-northeast-2': 'Asia Pacific (Seoul)',
            'ap-south-1': 'Asia Pacific (Mumbai)',
            'ca-central-1': 'Canada (Central)',
            'sa-east-1': 'South America (Sao Paulo)',
        }
        return region_mapping.get(self.region, 'US East (N. Virginia)')

    def calculate_mrr(self, hourly_rate) -> float:
        """计算月度经常性收入 (MRR) = 小时费率 * 24小时 * 30天"""
        if hourly_rate == 'NA' or hourly_rate is None:
            return 'NA'
        try:
            return round(float(hourly_rate) * 730, 4)
        except (ValueError, TypeError):
            return 'NA'

    def analyze_and_export(self, output_file: str = None):
        """分析RDS实例并导出到CSV文件"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"rds_mysql_extended_support_fee_{self.region}_{timestamp}.csv"
        
        print(f"开始分析 {self.region} 区域的RDS MySQL实例...")
        
        # 获取RDS实例信息
        instances = self.get_rds_instances()
        print(f"找到 {len(instances)} 个MySQL实例")
        
        if not instances:
            print("未找到MySQL实例")
            return
        
        # 批量获取所有实例类型的CPU核心数
        unique_instance_classes = list(set(instance['instance_class'] for instance in instances))
        cpu_cores_mapping = self.get_all_instance_cpu_cores(unique_instance_classes)
        
        # 更新实例信息中的CPU核心数
        for instance in instances:
            instance['cpu_cores'] = cpu_cores_mapping.get(instance['instance_class'], 2)
        
        # 一次性获取所有实例类型的RI定价
        ri_pricing_cache = self.get_all_ri_pricing()
        
        results = []
        
        for instance in instances:
            print(f"分析实例: {instance['db_instance_identifier']}")
            #print("##################")
            #print(instance)
            
            # 从缓存中获取RI定价
            ri_hourly_rate = ri_pricing_cache.get(instance['instance_class'], 0.0)
            #print("@@@@@@@@@@@@")
            #print(ri_hourly_rate)
            
            # 判断是否为Multi-AZ，如果是则RI费用乘以2
            multiplier = 2 if instance['architecture'] == 'MAZ' else 1
            adjusted_ri_hourly_rate = ri_hourly_rate * multiplier
            ri_mrr = self.calculate_mrr(adjusted_ri_hourly_rate)
            
            # 获取Extended Support定价
            extended_support_pricing = self.get_extended_support_pricing(instance['cpu_cores'])
            
            # 计算Extended Support MRR，Multi-AZ实例费用乘以2
            def calculate_extended_support_values(per_core_price, cpu_cores, multiplier):
                if per_core_price == 'NA':
                    return 'NA', 'NA'
                try:
                    total_hourly = float(per_core_price) * cpu_cores * multiplier
                    mrr = self.calculate_mrr(total_hourly)
                    return round(total_hourly, 4), mrr
                except (ValueError, TypeError):
                    return 'NA', 'NA'
            
            year1_total_hourly, year1_mrr = calculate_extended_support_values(
                extended_support_pricing['year1_per_core_per_hour'], instance['cpu_cores'], multiplier
            )
            year2_total_hourly, year2_mrr = calculate_extended_support_values(
                extended_support_pricing['year2_per_core_per_hour'], instance['cpu_cores'], multiplier
            )
            year3_total_hourly, year3_mrr = calculate_extended_support_values(
                extended_support_pricing['year3_per_core_per_hour'], instance['cpu_cores'], multiplier
            )
            
            # 计算比较百分比，处理NA值
            def calculate_percentage(es_mrr, ri_mrr):
                if es_mrr == 'NA' or ri_mrr == 'NA' or ri_mrr == 0:
                    return 'NA'
                try:
                    return f"{round(((es_mrr - ri_mrr) / ri_mrr) * 100, 3)}%"
                except (ValueError, TypeError, ZeroDivisionError):
                    return 'NA'
            
            ri_1yr_es_1yr = calculate_percentage(year1_mrr, ri_mrr) 
            ri_1yr_es_2yr = calculate_percentage(year2_mrr, ri_mrr) 
            ri_1yr_es_3yr = calculate_percentage(year3_mrr, ri_mrr) 

            result = {
                'account_id': instance['account_id'],
                'region': instance['region'],
                'db_instance_identifier': instance['db_instance_identifier'],
                'mysql_engine_version': instance['engine_version'],
                'architecture': instance['architecture'],
                'instance_class': instance['instance_class'],
                'cpu_cores': instance['cpu_cores'],
                '1yr_ri_noupfront_hourly_rate_usd': round(adjusted_ri_hourly_rate, 4),
                '1yr_ri_noupfront_mrr_usd': ri_mrr,
                'extended_support_year1_per_core_hourly_usd': extended_support_pricing['year1_per_core_per_hour'],
                #'extended_support_year1_total_hourly_usd': year1_total_hourly,
                'extended_support_year1_mrr_usd': year1_mrr,
                '1yr_extend_support vs. 1yr_ri_noupfront': ri_1yr_es_1yr,
                'extended_support_year2_per_core_hourly_usd': extended_support_pricing['year2_per_core_per_hour'],
                #'extended_support_year2_total_hourly_usd': year2_total_hourly,
                'extended_support_year2_mrr_usd': year2_mrr,
                '2yr_extend_support vs. 1yr_ri_noupfront':ri_1yr_es_2yr,
                'extended_support_year3_per_core_hourly_usd': extended_support_pricing['year3_per_core_per_hour'],
                #'extended_support_year3_total_hourly_usd': year3_total_hourly,
                'extended_support_year3_mrr_usd': year3_mrr,
                '3yr_extend_support vs. 1yr_ri_noupfront':ri_1yr_es_3yr
            }
            
            results.append(result)
        
        # 导出到CSV文件
        if results:
            self._export_to_csv(results, output_file)
            print(f"分析完成，结果已保存到: {output_file}")
        else:
            print("未找到MySQL实例或分析失败")

    def _export_to_csv(self, results: List[Dict], output_file: str):
        """导出结果到CSV文件"""
        fieldnames = [
            'account_id', 
            'region',
            'db_instance_identifier', 
            'mysql_engine_version', 
            'architecture', 
            'instance_class', 
            'cpu_cores',
            '1yr_ri_noupfront_hourly_rate_usd', 
            '1yr_ri_noupfront_mrr_usd',
            'extended_support_year1_per_core_hourly_usd', 
            #'extended_support_year1_total_hourly_usd', 
            'extended_support_year1_mrr_usd',
            '1yr_extend_support vs. 1yr_ri_noupfront',
            'extended_support_year2_per_core_hourly_usd', 
            #'extended_support_year2_total_hourly_usd', 
            'extended_support_year2_mrr_usd',
            '2yr_extend_support vs. 1yr_ri_noupfront',
            'extended_support_year3_per_core_hourly_usd', 
            #'extended_support_year3_total_hourly_usd', 
            'extended_support_year3_mrr_usd',
            '3yr_extend_support vs. 1yr_ri_noupfront'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

def main():
    parser = argparse.ArgumentParser(description='RDS MySQL实例定价分析工具')
    parser.add_argument('region', help='AWS区域名称 (例如: us-east-1)')
    parser.add_argument('-o', '--output', help='输出CSV文件名称')
    
    args = parser.parse_args()
    
    try:
        analyzer = RDSPricingAnalyzer(args.region)
        analyzer.analyze_and_export(args.output)
    except Exception as e:
        print(f"脚本执行失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()