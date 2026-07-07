#!/usr/bin/env python3
"""
Enhanced RDS Migration Cost Analyzer
支持查询指定region下的所有RDS和Aurora实例，并计算迁移到Graviton3/Graviton4的成本
功能：
1. 查询所有RDS实例（MySQL, PostgreSQL）和Aurora实例（MySQL, PostgreSQL）
2. 跳过T系列实例
3. 将micro, small, medium实例统一转换为large
4. 分别转换为M系列和R系列的Graviton3/Graviton4等价实例（m7g/m8g, r7g/r8g）
5. 将RDS和Aurora的成本计算结果分别保存在Excel文件的不同sheet中
"""

import boto3
import json
import argparse
from typing import Dict, List, Tuple, Optional, Any
from decimal import Decimal
import logging
import pandas as pd
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedRDSMigrationAnalyzer:
    def __init__(self, region: str = 'us-east-1'):
        self.region = region
        self.rds_client = boto3.client('rds', region_name=region)
        self.pricing_client = boto3.client('pricing', region_name='us-east-1')  # Pricing API只在us-east-1可用
        self.sts_client = boto3.client('sts', region_name=region)
        
        # 内置映射规则
        self.region_location_mapping = self._get_region_location_mapping()
        
        # 支持的引擎类型
        self.supported_engines = {
            'mysql': 'MySQL',
            'postgres': 'PostgreSQL',
            'aurora-mysql': 'Aurora MySQL',
            'aurora-postgresql': 'Aurora PostgreSQL'
        }

    def _get_region_location_mapping(self) -> Dict[str, str]:
        """AWS区域到定价API位置名称的映射"""
        return {
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

    def get_all_rds_instances(self) -> Tuple[List[Dict], List[Dict]]:
        """
        获取指定region下的所有RDS实例和Aurora实例
        返回: (rds_instances, aurora_instances)
        """
        rds_instances = []
        aurora_instances = []
        
        try:
            # 获取RDS实例
            logger.info(f"正在获取区域 {self.region} 的RDS实例...")
            paginator = self.rds_client.get_paginator('describe_db_instances')
            
            for page in paginator.paginate():
                for instance in page['DBInstances']:
                    engine = instance.get('Engine', '').lower()
                    
                    # 检查是否为Aurora实例
                    if engine.startswith('aurora'):
                        # Aurora实例，直接添加到aurora_instances列表
                        aurora_instances.append(instance)
                    else:
                        # 普通RDS实例
                        if engine in ['mysql', 'postgres']:
                            rds_instances.append(instance)
            
            logger.info(f"找到 {len(rds_instances)} 个RDS实例和 {len(aurora_instances)} 个Aurora实例")
            
        except Exception as e:
            logger.error(f"获取RDS实例失败: {e}")
            
        return rds_instances, aurora_instances

    def get_graviton_targets(self, instance_class: str, is_aurora: bool = False) -> List[Tuple[str, str, str]]:
        """
        根据实例类型获取Graviton3/Graviton4目标实例
        规则：
        1. 跳过T系列实例
        2. micro, small, medium 实例 → 统一转换为large
        3. RDS: 分别转换为M系列和R系列的Graviton3/Graviton4实例
        4. Aurora: 仅转换为R系列的Graviton3/Graviton4实例
        
        返回: [(series_type, graviton3_instance, graviton4_instance), ...]
        """
        family, size = self.extract_instance_family_and_size(instance_class)
        
        if not family:
            return []
        
        # 跳过T系列实例
        if family.startswith('t'):
            logger.info(f"跳过T系列实例: {instance_class}")
            return []
        
        # 确定目标实例大小 - micro, small, medium 统一转换为large
        if size in ['micro', 'small', 'medium']:
            target_size = 'large'
        else:
            target_size = size
        
        # 生成目标实例类型
        targets = []
        
        if is_aurora:
            # Aurora 仅转换为R系列 Graviton3/Graviton4
            r_graviton3_instance = f"db.r7g.{target_size}"
            r_graviton4_instance = f"db.r8g.{target_size}"
            targets.append(('R-series', r_graviton3_instance, r_graviton4_instance))
        else:
            # RDS 转换为M系列和R系列 Graviton3/Graviton4
            # M系列 Graviton3/Graviton4
            m_graviton3_instance = f"db.m7g.{target_size}"
            m_graviton4_instance = f"db.m8g.{target_size}"
            targets.append(('M-series', m_graviton3_instance, m_graviton4_instance))
            
            # R系列 Graviton3/Graviton4
            r_graviton3_instance = f"db.r7g.{target_size}"
            r_graviton4_instance = f"db.r8g.{target_size}"
            targets.append(('R-series', r_graviton3_instance, r_graviton4_instance))
        
        return targets

    def extract_instance_family_and_size(self, instance_class: str) -> Tuple[str, str]:
        """
        从实例类型中提取系列和大小
        例如: db.m5.large -> ('m5', 'large')
        """
        parts = instance_class.split('.')
        if len(parts) >= 3:
            return parts[1], parts[2]  # family, size
        return '', ''

    def get_rds_pricing(self, instance_class: str, engine: str = 'MySQL') -> Optional[float]:
        """获取RDS实例的1年RI无预付定价"""
        try:
            response = self.pricing_client.get_products(
                ServiceCode='AmazonRDS',
                Filters=[
                    {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_class},
                    {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': engine},
                    {'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Single-AZ'},
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_location_name()},
                ]
            )

            for price_item in response['PriceList']:
                price_data = json.loads(price_item)
                terms = price_data.get('terms', {})
                reserved_terms = terms.get('Reserved', {})
                
                for term_key, term_data in reserved_terms.items():
                    term_attributes = term_data.get('termAttributes', {})
                    if (term_attributes.get('LeaseContractLength') == '1yr' and 
                        term_attributes.get('PurchaseOption') == 'No Upfront'):
                        
                        price_dimensions = term_data.get('priceDimensions', {})
                        for price_key, price_info in price_dimensions.items():
                            price_per_unit = price_info.get('pricePerUnit', {})
                            usd_price = price_per_unit.get('USD')
                            if usd_price:
                                return float(usd_price)
            
            logger.warning(f"未找到 {instance_class} 的RDS定价信息")
            return None
            
        except Exception as e:
            logger.error(f"获取RDS定价失败 {instance_class}: {str(e)}")
            return None

    def get_aurora_pricing(self, instance_class: str, engine: str = 'Aurora MySQL') -> Optional[float]:
        """获取Aurora实例的1年RI无预付定价"""
        try:
            response = self.pricing_client.get_products(
                ServiceCode='AmazonRDS',
                Filters=[
                    {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_class},
                    {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': engine},
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_location_name()},
                ]
            )
            
            for price_item in response['PriceList']:
                price_data = json.loads(price_item)
                terms = price_data.get('terms', {})
                reserved_terms = terms.get('Reserved', {})
                
                for term_key, term_data in reserved_terms.items():
                    term_attributes = term_data.get('termAttributes', {})
                    if (term_attributes.get('LeaseContractLength') == '1yr' and 
                        term_attributes.get('PurchaseOption') == 'No Upfront'):
                        
                        price_dimensions = term_data.get('priceDimensions', {})
                        for price_key, price_info in price_dimensions.items():
                            price_per_unit = price_info.get('pricePerUnit', {})
                            usd_price = price_per_unit.get('USD')
                            if usd_price:
                                return float(usd_price)
            
            logger.warning(f"未找到 {instance_class} 的Aurora定价信息")
            return None
            
        except Exception as e:
            logger.error(f"获取Aurora定价失败 {instance_class}: {str(e)}")
            return None

    def _get_cluster_writer_node(self, cluster_identifier: str) -> str:
        """
        获取Multi-AZ DB集群中的writer node实例ID
        """
        try:
            # 获取集群中的所有实例
            response = self.rds_client.describe_db_instances(
                Filters=[
                    {
                        'Name': 'db-cluster-id',
                        'Values': [cluster_identifier]
                    }
                ]
            )
            
            for db_instance in response['DBInstances']:
                # 检查实例角色，寻找writer实例
                # Writer实例通常没有ReadReplicaSourceDBInstanceIdentifier
                # 并且在集群中扮演主要角色
                if not db_instance.get('ReadReplicaSourceDBInstanceIdentifier'):
                    # 进一步验证这是writer实例
                    # 可以通过检查实例的可用性状态和角色来确认
                    return db_instance.get('DBInstanceIdentifier', '')
            
            logger.warning(f"未找到集群 {cluster_identifier} 的writer node")
            return 'Unknown'
            
        except Exception as e:
            logger.error(f"获取集群 {cluster_identifier} 的writer node失败: {e}")
            return 'Unknown'

    def _get_location_name(self) -> str:
        """将AWS区域代码转换为定价API使用的位置名称"""
        return self.region_location_mapping.get(self.region, 'US East (N. Virginia)')

    def get_account_id(self) -> str:
        """获取当前AWS账户ID"""
        try:
            response = self.sts_client.get_caller_identity()
            return response.get('Account', 'Unknown')
        except Exception as e:
            logger.warning(f"无法获取账户ID: {e}")
            return 'Unknown'

    def analyze_rds_migration_costs(self, rds_instances: List[Dict]) -> List[Dict]:
        """分析RDS实例迁移到Graviton的成本"""
        results = []
        
        # 获取账户ID
        account_id = self.get_account_id()
        
        logger.info(f"分析 {len(rds_instances)} 个RDS实例的迁移成本...")
        
        for instance in rds_instances:
            instance_id = instance.get('DBInstanceIdentifier', 'Unknown')
            instance_class = instance.get('DBInstanceClass', '')
            engine = instance.get('Engine', '').lower()
            engine_version = instance.get('EngineVersion', '')
            multi_az = instance.get('MultiAZ', False)
            
            # 获取source_db_instance_identifier
            source_db_instance_identifier = ''
            
            # 检查是否为Multi-AZ DB集群（TAZ架构）
            db_cluster_identifier = instance.get('DBClusterIdentifier')
            
            if instance.get('ReadReplicaSourceDBInstanceIdentifier'):
                # Read Replica情况
                source_db_instance_identifier = instance.get('ReadReplicaSourceDBInstanceIdentifier', '')
            elif db_cluster_identifier:
                # TAZ架构（Multi-AZ DB集群），需要找到writer node
                source_db_instance_identifier = self._get_cluster_writer_node(db_cluster_identifier)
            elif multi_az:
                # MAZ架构（传统Multi-AZ实例部署）
                source_db_instance_identifier = 'NA'
            # 普通单AZ实例保持空字符串
            
            # 映射引擎名称
            engine_display = self.supported_engines.get(engine, engine)
            
            logger.info(f"  分析RDS实例: {instance_id} ({instance_class})")
            
            # 获取原始实例定价
            original_price = self.get_rds_pricing(instance_class, engine_display)
            original_mrr = original_price * 730 if original_price else 'NA'
            
            # 如果是Multi-AZ，价格需要乘以2
            if multi_az and original_price:
                original_price *= 2
                if original_mrr != 'NA':
                    original_mrr = original_price * 730
            
            # 获取Graviton目标实例 (返回M系列和R系列的Graviton3和Graviton4)
            graviton_targets = self.get_graviton_targets(instance_class, is_aurora=False)
            
            if not graviton_targets:
                logger.warning(f"跳过实例 {instance_class} (T系列或无法找到目标实例)")
                continue
            
            # 初始化结果字典
            result = {
                'account_id': account_id,
                'region': self.region,
                'instance_id': instance_id,
                'source_db_instance_identifier': source_db_instance_identifier,
                'engine': engine_display,
                'engine_version': engine_version,
                'multi_az': multi_az,
                'original_instance_class': instance_class,
                'original_hourly_price_usd': original_price if original_price else 'NA',
                'original_mrr_usd': original_mrr,
            }
            
            # 处理M系列和R系列的目标实例
            for series_type, graviton3_instance, graviton4_instance in graviton_targets:
                # 获取Graviton3定价
                graviton3_price = self.get_rds_pricing(graviton3_instance, engine_display)
                graviton3_mrr = graviton3_price * 730 if graviton3_price else 'NA'
                
                # 获取Graviton4定价
                graviton4_price = self.get_rds_pricing(graviton4_instance, engine_display)
                graviton4_mrr = graviton4_price * 730 if graviton4_price else 'NA'
                
                # 如果是Multi-AZ，目标价格也需要乘以2
                if multi_az:
                    if graviton3_price:
                        graviton3_price *= 2
                        if graviton3_mrr != 'NA':
                            graviton3_mrr = graviton3_price * 730
                    if graviton4_price:
                        graviton4_price *= 2
                        if graviton4_mrr != 'NA':
                            graviton4_mrr = graviton4_price * 730
                
                # 计算Graviton3成本差异
                graviton3_cost_diff_hourly = 'NA'
                graviton3_cost_diff_mrr = 'NA'
                graviton3_cost_diff_percentage = 'NA'
                
                if original_price and graviton3_price:
                    graviton3_cost_diff_hourly = graviton3_price - original_price
                    graviton3_cost_diff_mrr = graviton3_mrr - original_mrr if (graviton3_mrr != 'NA' and original_mrr != 'NA') else 'NA'
                    graviton3_cost_diff_percentage = f"{((graviton3_cost_diff_hourly / original_price) * 100):.2f}%"
                
                # 计算Graviton4成本差异
                graviton4_cost_diff_hourly = 'NA'
                graviton4_cost_diff_mrr = 'NA'
                graviton4_cost_diff_percentage = 'NA'
                
                if original_price and graviton4_price:
                    graviton4_cost_diff_hourly = graviton4_price - original_price
                    graviton4_cost_diff_mrr = graviton4_mrr - original_mrr if (graviton4_mrr != 'NA' and original_mrr != 'NA') else 'NA'
                    graviton4_cost_diff_percentage = f"{((graviton4_cost_diff_hourly / original_price) * 100):.2f}%"
                
                # 根据系列类型添加到结果字典
                if series_type == 'M-series':
                    result.update({
                        'm_graviton3_instance_class': graviton3_instance,
                        'm_graviton3_hourly_price_usd': graviton3_price if graviton3_price else 'NA',
                        'm_graviton3_mrr_usd': graviton3_mrr,
                        'm_graviton3_cost_diff_mrr_usd': graviton3_cost_diff_mrr,
                        'm_graviton3_cost_diff_percentage': graviton3_cost_diff_percentage,
                        'm_graviton4_instance_class': graviton4_instance,
                        'm_graviton4_hourly_price_usd': graviton4_price if graviton4_price else 'NA',
                        'm_graviton4_mrr_usd': graviton4_mrr,
                        'm_graviton4_cost_diff_hourly_usd': graviton4_cost_diff_hourly,
                        'm_graviton4_cost_diff_mrr_usd': graviton4_cost_diff_mrr,
                        'm_graviton4_cost_diff_percentage': graviton4_cost_diff_percentage
                    })
                elif series_type == 'R-series':
                    result.update({
                        'r_graviton3_instance_class': graviton3_instance,
                        'r_graviton3_hourly_price_usd': graviton3_price if graviton3_price else 'NA',
                        'r_graviton3_mrr_usd': graviton3_mrr,
                        'r_graviton3_cost_diff_hourly_usd': graviton3_cost_diff_hourly,
                        'r_graviton3_cost_diff_mrr_usd': graviton3_cost_diff_mrr,
                        'r_graviton3_cost_diff_percentage': graviton3_cost_diff_percentage,
                        'r_graviton4_instance_class': graviton4_instance,
                        'r_graviton4_hourly_price_usd': graviton4_price if graviton4_price else 'NA',
                        'r_graviton4_mrr_usd': graviton4_mrr,
                        'r_graviton4_cost_diff_hourly_usd': graviton4_cost_diff_hourly,
                        'r_graviton4_cost_diff_mrr_usd': graviton4_cost_diff_mrr,
                        'r_graviton4_cost_diff_percentage': graviton4_cost_diff_percentage
                    })
            
            results.append(result)
        
        return results

    def analyze_aurora_migration_costs(self, aurora_instances: List[Dict]) -> List[Dict]:
        """分析Aurora实例迁移到Graviton的成本"""
        results = []
        
        # 获取账户ID
        account_id = self.get_account_id()
        
        logger.info(f"分析 {len(aurora_instances)} 个Aurora实例的迁移成本...")
        
        for instance in aurora_instances:
            instance_id = instance.get('DBInstanceIdentifier', 'Unknown')
            cluster_id = instance.get('DBClusterIdentifier', 'Unknown')
            instance_class = instance.get('DBInstanceClass', '')
            engine = instance.get('Engine', '').lower()
            engine_version = instance.get('EngineVersion', '')
            
            # 获取集群信息以检查是否为Serverless
            try:
                cluster_response = self.rds_client.describe_db_clusters(
                    DBClusterIdentifier=cluster_id
                )
                if cluster_response['DBClusters']:
                    cluster_info = cluster_response['DBClusters'][0]
                    
                    # 检查是否为Aurora Serverless，如果是则跳过
                    engine_mode = cluster_info.get('EngineMode', '')
                    serverless_v2_scaling = cluster_info.get('ServerlessV2ScalingConfiguration')
                    
                    # Aurora Serverless v1 检查
                    if engine_mode == 'serverless':
                        logger.info(f"  跳过Aurora Serverless v1实例: {instance_id} (集群: {cluster_id})")
                        continue
                    
                    # Aurora Serverless v2 检查
                    if serverless_v2_scaling is not None:
                        logger.info(f"  跳过Aurora Serverless v2实例: {instance_id} (集群: {cluster_id})")
                        continue
                        
            except Exception as e:
                logger.warning(f"无法获取Aurora集群 {cluster_id} 的信息: {e}")
                # 如果无法获取集群信息，继续处理实例
            
            if not instance_class:
                logger.warning(f"  Aurora实例 {instance_id} 没有实例类型信息，跳过")
                continue
            
            # 获取source_db_instance_identifier
            source_db_instance_identifier = ''
            
            # 检查实例角色
            if instance.get('ReadReplicaSourceDBInstanceIdentifier'):
                # 这是一个read replica实例
                source_db_instance_identifier = instance.get('ReadReplicaSourceDBInstanceIdentifier', '')
            else:
                # 这可能是writer实例，source_db_instance_identifier保持为空
                source_db_instance_identifier = ''
            
            # 映射引擎名称
            engine_display = self.supported_engines.get(engine, engine)
            
            logger.info(f"  分析Aurora实例: {instance_id} ({instance_class}) - 集群: {cluster_id}")
            
            # 获取原始实例定价
            original_price = self.get_aurora_pricing(instance_class, engine_display)
            original_mrr = original_price * 730 if original_price else 'NA'
            
            # 获取Graviton目标实例 (Aurora只返回R系列)
            graviton_targets = self.get_graviton_targets(instance_class, is_aurora=True)
            
            if not graviton_targets:
                logger.warning(f"跳过实例 {instance_class} (T系列或无法找到目标实例)")
                continue
            
            # 初始化结果字典
            result = {
                'account_id': account_id,
                'region': self.region,
                'instance_id': instance_id,
                'cluster_id': cluster_id,
                #'source_db_instance_identifier': source_db_instance_identifier,
                'engine': engine_display,
                'engine_version': engine_version,
                'original_instance_class': instance_class,
                'original_hourly_price_usd': original_price if original_price else 'NA',
                'original_mrr_usd': original_mrr,
            }
            
            # 处理R系列的目标实例（Aurora只转换为R系列）
            for series_type, graviton3_instance, graviton4_instance in graviton_targets:
                # 获取Graviton3定价
                graviton3_price = self.get_aurora_pricing(graviton3_instance, engine_display)
                graviton3_mrr = graviton3_price * 730 if graviton3_price else 'NA'
                
                # 获取Graviton4定价
                graviton4_price = self.get_aurora_pricing(graviton4_instance, engine_display)
                graviton4_mrr = graviton4_price * 730 if graviton4_price else 'NA'
                
                # 计算Graviton3成本差异
                graviton3_cost_diff_hourly = 'NA'
                graviton3_cost_diff_mrr = 'NA'
                graviton3_cost_diff_percentage = 'NA'
                
                if original_price and graviton3_price:
                    graviton3_cost_diff_hourly = graviton3_price - original_price
                    graviton3_cost_diff_mrr = graviton3_mrr - original_mrr if (graviton3_mrr != 'NA' and original_mrr != 'NA') else 'NA'
                    graviton3_cost_diff_percentage = f"{((graviton3_cost_diff_hourly / original_price) * 100):.2f}%"
                
                # 计算Graviton4成本差异
                graviton4_cost_diff_hourly = 'NA'
                graviton4_cost_diff_mrr = 'NA'
                graviton4_cost_diff_percentage = 'NA'
                
                if original_price and graviton4_price:
                    graviton4_cost_diff_hourly = graviton4_price - original_price
                    graviton4_cost_diff_mrr = graviton4_mrr - original_mrr if (graviton4_mrr != 'NA' and original_mrr != 'NA') else 'NA'
                    graviton4_cost_diff_percentage = f"{((graviton4_cost_diff_hourly / original_price) * 100):.2f}%"
                
                # Aurora只有R系列，直接添加R系列字段（去掉graviton3_cost_diff_hourly_usd）
                result.update({
                    'graviton3_instance_class': graviton3_instance,
                    'graviton3_hourly_price_usd': graviton3_price if graviton3_price else 'NA',
                    'graviton3_mrr_usd': graviton3_mrr,
                    'graviton3_cost_diff_mrr_usd': graviton3_cost_diff_mrr,
                    'graviton3_cost_diff_percentage': graviton3_cost_diff_percentage,
                    'graviton4_instance_class': graviton4_instance,
                    'graviton4_hourly_price_usd': graviton4_price if graviton4_price else 'NA',
                    'graviton4_mrr_usd': graviton4_mrr,
                    'graviton4_cost_diff_hourly_usd': graviton4_cost_diff_hourly,
                    'graviton4_cost_diff_mrr_usd': graviton4_cost_diff_mrr,
                    'graviton4_cost_diff_percentage': graviton4_cost_diff_percentage
                })
            
            results.append(result)
        
        return results

    def export_to_excel(self, rds_results: List[Dict], aurora_results: List[Dict], output_file: str):
        """将分析结果导出到Excel文件，RDS和Aurora分别在不同的sheet"""
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # RDS迁移成本分析sheet
            if rds_results:
                rds_df = pd.DataFrame(rds_results)
                rds_df.to_excel(writer, sheet_name='RDS Migration Costs', index=False)
                logger.info(f"RDS迁移成本分析已导出到 'RDS Migration Costs' sheet，共 {len(rds_results)} 条记录")
            else:
                # 创建空的RDS sheet
                empty_rds_df = pd.DataFrame(columns=[
                    'account_id', 
                    'region', 
                    'instance_id',
                    'source_db_instance_identifier',
                    'engine', 
                    'engine_version', 
                    'multi_az',
                    'original_instance_class', 
                    'original_hourly_price_usd', 
                    'original_mrr_usd',
                    'm_graviton3_instance_class',
                    'm_graviton3_hourly_price_usd', 
                    'm_graviton3_mrr_usd',
                    'm_graviton3_cost_diff_mrr_usd', 
                    'm_graviton3_cost_diff_percentage',
                    'm_graviton4_instance_class',
                    'm_graviton4_hourly_price_usd', 
                    'm_graviton4_mrr_usd',
                    'm_graviton4_cost_diff_hourly_usd', 
                    'm_graviton4_cost_diff_mrr_usd', 
                    'm_graviton4_cost_diff_percentage',
                    'r_graviton3_instance_class',
                    'r_graviton3_hourly_price_usd', 
                    'r_graviton3_mrr_usd',
                    'r_graviton3_cost_diff_hourly_usd', 
                    'r_graviton3_cost_diff_mrr_usd', 
                    'r_graviton3_cost_diff_percentage',
                    'r_graviton4_instance_class',
                    'r_graviton4_hourly_price_usd', 
                    'r_graviton4_mrr_usd',
                    'r_graviton4_cost_diff_hourly_usd', 
                    'r_graviton4_cost_diff_mrr_usd', 
                    'r_graviton4_cost_diff_percentage'
                ])
                empty_rds_df.to_excel(writer, sheet_name='RDS Migration Costs', index=False)
                logger.info("未找到RDS实例，创建空的RDS sheet")
            
            # Aurora迁移成本分析sheet
            if aurora_results:
                aurora_df = pd.DataFrame(aurora_results)
                aurora_df.to_excel(writer, sheet_name='Aurora Migration Costs', index=False)
                logger.info(f"Aurora迁移成本分析已导出到 'Aurora Migration Costs' sheet，共 {len(aurora_results)} 条记录")
            else:
                # 创建空的Aurora sheet
                empty_aurora_df = pd.DataFrame(columns=[
                    'account_id', 
                    'region',
                    'instance_id',
                    'cluster_id',
                    #'source_db_instance_identifier',
                    'engine', 
                    'engine_version',
                    'original_instance_class', 
                    'original_hourly_price_usd', 
                    'original_mrr_usd',
                    'graviton3_instance_class', 
                    'graviton3_hourly_price_usd', 
                    'graviton3_mrr_usd',
                    'graviton3_cost_diff_mrr_usd', 
                    'graviton3_cost_diff_percentage',
                    'graviton4_instance_class',
                    'graviton4_hourly_price_usd', 
                    'graviton4_mrr_usd',
                    'graviton4_cost_diff_hourly_usd', 
                    'graviton4_cost_diff_mrr_usd', 
                    'graviton4_cost_diff_percentage'
                ])
                empty_aurora_df.to_excel(writer, sheet_name='Aurora Migration Costs', index=False)
                logger.info("未找到Aurora集群，创建空的Aurora sheet")
        
        logger.info(f"分析结果已导出到: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Enhanced RDS Migration Cost Analyzer - 支持查询所有RDS和Aurora实例并计算Graviton迁移成本')
    parser.add_argument('region', help='AWS区域 (例如: us-east-1, ap-southeast-1)')
    parser.add_argument('-o', '--output', default=None,
                       help='输出Excel文件名 (默认: rds_migration_analysis_{region}_{timestamp}.xlsx)')
    
    args = parser.parse_args()
    
    # 生成默认输出文件名
    if not args.output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output = f'rds_migration_analysis_{args.region}_{timestamp}.xlsx'
    
    # 创建分析器
    analyzer = EnhancedRDSMigrationAnalyzer(args.region)
    
    # 获取所有RDS实例和Aurora实例
    logger.info(f"开始分析区域 {args.region} 的RDS和Aurora实例...")
    rds_instances, aurora_instances = analyzer.get_all_rds_instances()
    
    if not rds_instances and not aurora_instances:
        logger.warning(f"在区域 {args.region} 中未找到任何RDS实例或Aurora实例")
        return
    
    # 分析RDS迁移成本
    rds_results = []
    if rds_instances:
        logger.info("开始分析RDS实例迁移成本...")
        rds_results = analyzer.analyze_rds_migration_costs(rds_instances)
    
    # 分析Aurora迁移成本
    aurora_results = []
    if aurora_instances:
        logger.info("开始分析Aurora实例迁移成本...")
        aurora_results = analyzer.analyze_aurora_migration_costs(aurora_instances)
    
    # 导出结果到Excel
    analyzer.export_to_excel(rds_results, aurora_results, args.output)
    
    # 打印汇总信息
    logger.info("="*80)
    logger.info("分析完成汇总:")
    logger.info(f"区域: {args.region}")
    logger.info(f"RDS实例: {len(rds_instances)} 个")
    logger.info(f"Aurora实例: {len(aurora_instances)} 个")
    logger.info(f"RDS迁移方案: {len(rds_results)} 个")
    logger.info(f"Aurora迁移方案: {len(aurora_results)} 个")
    logger.info(f"结果文件: {args.output}")
    logger.info("="*80)

if __name__ == "__main__":
    main()
