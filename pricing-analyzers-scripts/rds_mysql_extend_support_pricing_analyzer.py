#!/usr/bin/env python3
"""
RDS MySQL实例定价分析脚本 - API版本
通过AWS Pricing API获取所有region的Extended Support定价
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
        
        # 区域兼容性检查
        if not self._check_region_compatibility():
            print("\n❌ 脚本因区域兼容性问题终止执行")
            sys.exit(1)
        
        self.rds_client = boto3.client('rds', region_name=region)
        self.pricing_client = boto3.client('pricing', region_name='us-east-1')
        self.sts_client = boto3.client('sts', region_name=region)
        
        # 缓存
        self.cpu_cores_cache = {}
        self._extended_support_cache = {}

    def _get_current_ec2_region(self):
        """获取当前EC2实例所在的区域"""
        try:
            import requests
            # 尝试IMDSv2
            token_response = requests.put(
                'http://169.254.169.254/latest/api/token',
                headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'},
                timeout=2
            )
            if token_response.status_code == 200:
                token = token_response.text
                region_response = requests.get(
                    'http://169.254.169.254/latest/meta-data/placement/region',
                    headers={'X-aws-ec2-metadata-token': token},
                    timeout=2
                )
                if region_response.status_code == 200:
                    return region_response.text.strip()
            
            # 回退到IMDSv1
            region_response = requests.get(
                'http://169.254.169.254/latest/meta-data/placement/region',
                timeout=2
            )
            if region_response.status_code == 200:
                return region_response.text.strip()
                
        except Exception:
            pass
        
        # 如果IMDS失败，尝试通过STS ARN推断
        try:
            sts_client = boto3.client('sts')
            identity = sts_client.get_caller_identity()
            arn = identity.get('Arn', '')
            if ':cn-' in arn:
                return 'cn-north-1'  # 默认中国区域
            else:
                return 'us-east-1'  # 默认全球区域
        except:
            return None

    def _is_china_region(self, region):
        """判断是否为中国区域"""
        return region.startswith('cn-') if region else False

    def _check_region_compatibility(self):
        """检查区域兼容性"""
        current_region = self._get_current_ec2_region()
        
        if not current_region:
            print("⚠️  警告: 无法确定当前EC2实例所在区域，跳过兼容性检查")
            return True
        
        current_is_china = self._is_china_region(current_region)
        target_is_china = self._is_china_region(self.region)
        
        # 当前EC2在中国区域时，直接提示不支持
        if current_is_china:
            print(f"""
❌ 不支持的运行环境！

当前EC2实例位于: {current_region} (中国区域)
此脚本不支持在中国区域运行。
""")
            return False
        
        # 当前EC2在全球区域，但目标是中国区域时的提示
        elif not current_is_china and target_is_china:
            print(f"""
❌ 区域兼容性错误！

当前EC2实例位于: {current_region} (全球区域)
目标评估区域: {self.region} (中国区域)

⚠️  在全球区域的EC2实例上无法评估中国区域的RDS实例！
请在中国区域的EC2实例上运行此脚本来分析中国区域的RDS实例。
""")
            return False
        
        # 全球区域到全球区域
        print(f"✅ 区域兼容性检查通过: {current_region} -> {self.region} (全球分区)")
        return True

    def get_extended_support_pricing(self, cpu_cores: int) -> Dict[str, float]:
        """通过AWS Pricing API获取Extended Support每核心每小时的定价"""
        cache_key = f"extended_support_{self.region}"
        if cache_key in self._extended_support_cache:
            return self._extended_support_cache[cache_key]
        
        try:
            print(f"正在获取 {self.region} 的Extended Support定价...")
            
            # 使用分页获取所有Extended Support相关产品
            paginator = self.pricing_client.get_paginator('get_products')
            page_iterator = paginator.paginate(
                ServiceCode='AmazonRDS',
                Filters=[
                    {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'}
                ]
            )
            
            pricing_data = {
                'year1_per_core_per_hour': 0.0,
                'year2_per_core_per_hour': 0.0,
                'year3_per_core_per_hour': 0.0,
            }
            
            location = self._get_location_name()
            
            for page in page_iterator:
                for product_str in page['PriceList']:
                    product = json.loads(product_str)
                    attributes = product['product']['attributes']
                    
                    # 检查是否为Extended Support产品且匹配当前region
                    usage_type = attributes.get('usagetype', '')
                    product_location = attributes.get('location', '')
                    
                    if 'ExtendedSupport' in usage_type and product_location == location:
                        # 只获取标准版Extended Support定价，过滤特殊后缀版本
                        if ':PP' in usage_type or ':Gov' in usage_type or ':Spot' in usage_type:
                            continue
                        
                        # 获取定价
                        terms = product.get('terms', {}).get('OnDemand', {})
                        
                        for term_key, term_data in terms.items():
                            price_dimensions = term_data.get('priceDimensions', {})
                            for price_key, price_data in price_dimensions.items():
                                price_per_unit = price_data.get('pricePerUnit', {}).get('USD', '0')
                                
                                if float(price_per_unit) > 0:
                                    price_value = float(price_per_unit)
                                    
                                    # 根据usage_type判断年份
                                    if 'Yr1-Yr2' in usage_type:
                                        pricing_data['year1_per_core_per_hour'] = price_value
                                        pricing_data['year2_per_core_per_hour'] = price_value
                                    elif 'Yr3' in usage_type:
                                        pricing_data['year3_per_core_per_hour'] = price_value
            
            # 缓存结果
            self._extended_support_cache[cache_key] = pricing_data
            
            # 如果没有找到定价，返回NA
            if all(price == 0.0 for price in pricing_data.values()):
                pricing_data = {
                    'year1_per_core_per_hour': 'NA',
                    'year2_per_core_per_hour': 'NA',
                    'year3_per_core_per_hour': 'NA',
                }
                print(f"警告: 无法从API获取 {self.region} 的Extended Support定价")
            else:
                print(f"成功获取 {self.region} 的Extended Support定价: Year1-2=${pricing_data['year1_per_core_per_hour']}, Year3=${pricing_data['year3_per_core_per_hour']}")
            
            return pricing_data
            
        except Exception as e:
            print(f"获取Extended Support定价失败: {e}")
            return {
                'year1_per_core_per_hour': 'NA',
                'year2_per_core_per_hour': 'NA',
                'year3_per_core_per_hour': 'NA',
            }

    def get_instance_cpu_cores(self, instance_class: str) -> int:
        """获取实例类型的CPU核心数"""
        if instance_class in self.cpu_cores_cache:
            return self.cpu_cores_cache[instance_class]
        
        try:
            # 从实例类型名称推断CPU核心数
            cpu_cores = self._infer_cpu_cores_from_instance_name(instance_class)
            if cpu_cores > 0:
                self.cpu_cores_cache[instance_class] = cpu_cores
                return cpu_cores
            
            # 使用默认值
            self.cpu_cores_cache[instance_class] = 2
            return 2
            
        except Exception as e:
            print(f"获取实例类型 {instance_class} 的CPU核心数失败: {e}")
            self.cpu_cores_cache[instance_class] = 2
            return 2

    def _infer_cpu_cores_from_instance_name(self, instance_class: str) -> int:
        """从实例类型名称推断CPU核心数"""
        try:
            parts = instance_class.split('.')
            if len(parts) != 3:
                return 0
            
            family = parts[1]
            size = parts[2]
            
            size_mapping = {
                'nano': 1, 'micro': 1, 'small': 1, 'medium': 1, 'large': 2,
                'xlarge': 4, '2xlarge': 8, '3xlarge': 12, '4xlarge': 16,
                '6xlarge': 24, '8xlarge': 32, '9xlarge': 36, '10xlarge': 40,
                '12xlarge': 48, '16xlarge': 64, '18xlarge': 72, '24xlarge': 96,
                '32xlarge': 128, '48xlarge': 192,
            }
            
            if family.startswith('t'):
                if size in ['micro', 'small', 'medium', 'large']:
                    return 2
                elif size == 'xlarge':
                    return 4
                elif size == '2xlarge':
                    return 8
            
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
        """获取指定region下的所有RDS MySQL实例和集群节点"""
        instances = []
        try:
            # 获取RDS实例
            paginator = self.rds_client.get_paginator('describe_db_instances')
            
            for page in paginator.paginate():
                # 获取当前页面的所有实例用于架构判断
                all_page_instances = page['DBInstances']
                
                for db_instance in page['DBInstances']:
                    if db_instance['Engine'].lower() == 'mysql':
                        architecture = self._determine_architecture(db_instance, all_page_instances)
                        
                        # 设置source_db_instance_identifier
                        source_db_instance = ''
                        if db_instance.get('ReadReplicaSourceDBInstanceIdentifier'):
                            source_db_instance = db_instance['ReadReplicaSourceDBInstanceIdentifier']
                        elif db_instance.get('ReadReplicaSourceDBClusterIdentifier'):
                            # 集群级Read Replica，需要找到源集群的Writer节点
                            source_cluster = db_instance['ReadReplicaSourceDBClusterIdentifier']
                            try:
                                cluster_response = self.rds_client.describe_db_clusters(DBClusterIdentifier=source_cluster)
                                cluster = cluster_response['DBClusters'][0]
                                for member in cluster.get('DBClusterMembers', []):
                                    if member.get('IsClusterWriter', False):
                                        source_db_instance = member['DBInstanceIdentifier']
                                        break
                            except Exception as e:
                                print(f"获取源集群Writer失败: {e}")
                        
                        instance_info = {
                            'db_instance_identifier': db_instance['DBInstanceIdentifier'],
                            'account_id': self.get_account_id(),
                            'region': self.region,
                            'engine_version': db_instance['EngineVersion'],
                            'architecture': architecture,
                            'instance_class': db_instance['DBInstanceClass'],
                            'instance_status': db_instance.get('DBInstanceStatus', ''),
                            'cpu_cores': self.get_instance_cpu_cores(db_instance['DBInstanceClass']),
                            'multi_az': db_instance.get('MultiAZ', False),
                            'read_replica_source': db_instance.get('ReadReplicaSourceDBInstanceIdentifier', ''),
                            'source_db_instance_identifier': source_db_instance,
                            'availability_zone': db_instance.get('AvailabilityZone', ''),
                            'db_cluster_identifier': db_instance.get('DBClusterIdentifier', ''),
                        }
                        instances.append(instance_info)
            
            # 获取RDS集群
            try:
                cluster_paginator = self.rds_client.get_paginator('describe_db_clusters')
                
                for page in cluster_paginator.paginate():
                    for db_cluster in page['DBClusters']:
                        if db_cluster['Engine'].lower() == 'mysql':
                            cluster_members = db_cluster.get('DBClusterMembers', [])
                            
                            for member in cluster_members:
                                member_instance_id = member['DBInstanceIdentifier']
                                
                                # 检查是否已经在实例列表中
                                existing_instance = next(
                                    (inst for inst in instances if inst['db_instance_identifier'] == member_instance_id), 
                                    None
                                )
                                
                                if existing_instance:
                                    # 更新集群信息
                                    existing_instance['is_cluster_member'] = True
                                    existing_instance['cluster_architecture'] = 'multi_az_cluster' if db_cluster.get('MultiAZ', False) else 'single_az_cluster'
                                    existing_instance['is_cluster_writer'] = member.get('IsClusterWriter', False)
                                    
            except Exception as e:
                print(f"获取RDS集群信息失败: {e}")
                        
        except Exception as e:
            print(f"获取RDS实例失败: {e}")
        
        # 处理TAZ集群的source关系
        self._set_taz_source_relationships(instances)
            
        return instances

    def _set_taz_source_relationships(self, instances):
        """为TAZ集群设置source关系和架构"""
        # 按集群分组
        taz_clusters = {}
        for instance in instances:
            cluster_id = instance.get('db_cluster_identifier', '')
            if cluster_id and 'taz' in cluster_id.lower():
                if cluster_id not in taz_clusters:
                    taz_clusters[cluster_id] = []
                taz_clusters[cluster_id].append(instance)
        
        # 为每个TAZ集群设置source关系和架构
        for cluster_id, cluster_instances in taz_clusters.items():
            # 找到writer实例作为primary
            writer_instance = None
            for instance in cluster_instances:
                if instance.get('is_cluster_writer', False):
                    writer_instance = instance
                    instance['architecture'] = 'taz_cluster_writer'
                    break
            
            if writer_instance:
                writer_id = writer_instance['db_instance_identifier']
                # 将非writer实例设置为read replica
                for instance in cluster_instances:
                    if not instance.get('is_cluster_writer', False):
                        instance['source_db_instance_identifier'] = writer_id
                        instance['architecture'] = 'taz_cluster_reader'

    def _determine_architecture(self, db_instance: Dict, all_instances: List[Dict] = None) -> str:
        """优化的架构判断 - 仅通过API，正确识别TAZ和主从关系"""
        
        # 1. 检查实例级Read Replica
        if db_instance.get('ReadReplicaSourceDBInstanceIdentifier'):
            return 'read_replica_maz' if db_instance.get('MultiAZ', False) else 'read_replica'
        
        # 2. 检查集群级Read Replica
        if db_instance.get('ReadReplicaSourceDBClusterIdentifier'):
            return 'read_replica_maz' if db_instance.get('MultiAZ', False) else 'read_replica'
        
        # 3. 集群架构判断
        cluster_id = db_instance.get('DBClusterIdentifier')
        if cluster_id and all_instances:
            try:
                response = self.rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)
                cluster = response['DBClusters'][0]
                members = cluster.get('DBClusterMembers', [])
                
                # TAZ: 3成员 + 1Writer/2Reader + 跨3AZ
                if (len(members) == 3 and 
                    len([m for m in members if m.get('IsClusterWriter')]) == 1 and
                    self._is_cross_three_az(members, all_instances)):
                    return 'taz_cluster'
                    
                return 'multi_az' if cluster.get('MultiAZ') else 'single_az'
            except Exception as e:
                print(f"获取集群信息失败: {e}")
        
        # 4. 实例级MultiAZ
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

    def get_all_ri_pricing(self) -> Dict[str, float]:
        """获取该region下所有MySQL实例类型的1年RI无预付价格"""
        ri_pricing_cache = {}
        
        try:
            print("正在获取RI定价...")
            
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'serviceCode', 'Value': 'AmazonRDS'},
                {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': 'MySQL'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_location_name()},
                {'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Single-AZ'},
            ]
            
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
                        continue
                    
                    reserved_terms = product.get('terms', {}).get('Reserved', {})
                    for term in reserved_terms.values():
                        term_attrs = term.get('termAttributes', {})
                        if (term_attrs.get('LeaseContractLength') == '1yr' and 
                            term_attrs.get('PurchaseOption') == 'No Upfront'):
                            
                            for price_dim in term.get('priceDimensions', {}).values():
                                if price_dim.get('unit') == 'Hrs':
                                    price = float(price_dim.get('pricePerUnit', {}).get('USD', 0))
                                    if price > 0:
                                        ri_pricing_cache[instance_type] = price
                                    break
                
            print(f"成功获取 {len(ri_pricing_cache)} 个实例类型的RI定价")
            
        except Exception as e:
            print(f"获取RI定价失败: {e}")

        return ri_pricing_cache

    def _get_location_name(self) -> str:
        """将AWS region转换为Pricing API使用的location名称"""
        region_mapping = {
            'us-east-1': 'US East (N. Virginia)',
            'us-east-2': 'US East (Ohio)',
            'us-west-1': 'US West (N. California)',
            'us-west-2': 'US West (Oregon)',
            'eu-west-1': 'EU (Ireland)',
            'eu-west-2': 'EU (London)',
            'eu-west-3': 'EU (Paris)',
            'eu-central-1': 'EU (Frankfurt)',
            'eu-central-2': 'Europe (Zurich)',
            'eu-north-1': 'EU (Stockholm)',
            'eu-south-1': 'EU (Milan)',
            'eu-south-2': 'Europe (Spain)',
            'ap-southeast-1': 'Asia Pacific (Singapore)',
            'ap-southeast-2': 'Asia Pacific (Sydney)',
            'ap-southeast-3': 'Asia Pacific (Jakarta)',
            'ap-southeast-4': 'Asia Pacific (Melbourne)',
            'ap-southeast-5': 'Asia Pacific (Malaysia)',
            'ap-southeast-6': 'Asia Pacific (New Zealand)',
            'ap-southeast-7': 'Asia Pacific (Thailand)',
            'ap-northeast-1': 'Asia Pacific (Tokyo)',
            'ap-northeast-2': 'Asia Pacific (Seoul)',
            'ap-northeast-3': 'Asia Pacific (Osaka)',
            'ap-south-1': 'Asia Pacific (Mumbai)',
            'ap-south-2': 'Asia Pacific (Hyderabad)',
            'ap-east-1': 'Asia Pacific (Hong Kong)',
            'ap-east-2': 'Asia Pacific (Taipei)',
            'ca-central-1': 'Canada (Central)',
            'ca-west-1': 'Canada West (Calgary)',
            'sa-east-1': 'South America (Sao Paulo)',
            'af-south-1': 'Africa (Cape Town)',
            'me-central-1': 'Middle East (UAE)',
            'me-south-1': 'Middle East (Bahrain)',
            'il-central-1': 'Israel (Tel Aviv)',
            'mx-central-1': 'Mexico (Central)',
        }
        return region_mapping.get(self.region, 'US East (N. Virginia)')

    def calculate_mrr(self, hourly_rate) -> float:
        """计算月度经常性收入 (MRR)"""
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
        
        instances = self.get_rds_instances()
        print(f"找到 {len(instances)} 个MySQL实例")
        
        if not instances:
            print("未找到MySQL实例")
            return
        
        ri_pricing_cache = self.get_all_ri_pricing()
        
        results = []
        
        for instance in instances:
            print(f"分析实例: {instance['db_instance_identifier']}")
            
            ri_hourly_rate = ri_pricing_cache.get(instance['instance_class'], 0.0)
            
            # 确定最终架构类型和multiplier
            final_architecture = instance['architecture']
            multiplier = 1
            
            # 根据架构调整价格
            architecture = instance['architecture']
            if architecture in ['taz_cluster_writer', 'taz_cluster_reader']:
                multiplier = 1  # TAZ集群中每个实例单独计算
            elif architecture in ['multi_az', 'read_replica_maz']:
                multiplier = 2  # Multi-AZ实例乘以2
            else:
                multiplier = 1  # Single-AZ或Read Replica实例
            
            adjusted_ri_hourly_rate = ri_hourly_rate * multiplier
            ri_mrr = self.calculate_mrr(adjusted_ri_hourly_rate)
            
            # 获取Extended Support定价
            extended_support_pricing = self.get_extended_support_pricing(instance['cpu_cores'])
            
            # 计算Extended Support MRR
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
            
            # 计算比较百分比
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
                'source_db_instance_identifier': instance['source_db_instance_identifier'],
                'mysql_engine_version': instance['engine_version'],
                'architecture': final_architecture,
                'instance_class': instance['instance_class'],
                'instance_status': instance.get('instance_status', ''),
                'cpu_cores': instance['cpu_cores'],
                'multiplier': multiplier,
                '1yr_ri_noupfront_hourly_rate_usd': round(adjusted_ri_hourly_rate, 4),
                '1yr_ri_noupfront_mrr_usd': ri_mrr,
                'extended_support_year1_per_core_hourly_usd': extended_support_pricing['year1_per_core_per_hour'],
                'extended_support_year1_mrr_usd': year1_mrr,
                '1yr_extend_support vs. 1yr_ri_noupfront': ri_1yr_es_1yr,
                'extended_support_year2_per_core_hourly_usd': extended_support_pricing['year2_per_core_per_hour'],
                'extended_support_year2_mrr_usd': year2_mrr,
                '2yr_extend_support vs. 1yr_ri_noupfront': ri_1yr_es_2yr,
                'extended_support_year3_per_core_hourly_usd': extended_support_pricing['year3_per_core_per_hour'],
                'extended_support_year3_mrr_usd': year3_mrr,
                '3yr_extend_support vs. 1yr_ri_noupfront': ri_1yr_es_3yr
            }
            
            results.append(result)
        
        if results:
            self._export_to_csv(results, output_file)
            print(f"分析完成，结果已保存到: {output_file}")
        else:
            print("未找到MySQL实例或分析失败")

    def _export_to_csv(self, results: List[Dict], output_file: str):
        """导出结果到CSV文件"""
        fieldnames = [
            'account_id', 'region', 'db_instance_identifier', 'source_db_instance_identifier', 'mysql_engine_version', 
            'architecture', 'instance_class', 'instance_status', 'cpu_cores', 'multiplier',
            '1yr_ri_noupfront_hourly_rate_usd', '1yr_ri_noupfront_mrr_usd',
            'extended_support_year1_per_core_hourly_usd', 'extended_support_year1_mrr_usd',
            '1yr_extend_support vs. 1yr_ri_noupfront',
            'extended_support_year2_per_core_hourly_usd', 'extended_support_year2_mrr_usd',
            '2yr_extend_support vs. 1yr_ri_noupfront',
            'extended_support_year3_per_core_hourly_usd', 'extended_support_year3_mrr_usd',
            '3yr_extend_support vs. 1yr_ri_noupfront'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

def main():
    parser = argparse.ArgumentParser(description='RDS MySQL实例定价分析工具 - API版本')
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

