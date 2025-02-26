from typing import List, Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import json
from io import StringIO
from . import Connector

class AWSConnector(Connector):
    """Connector for AWS data services (S3, Redshift, DynamoDB)."""
    
    def __init__(
        self,
        service: str,  # s3, redshift, dynamodb
        region: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        **service_params
    ):
        """
        Initialize AWS Connector.
        
        Args:
            service: AWS service to connect to (s3, redshift, dynamodb)
            region: AWS region
            aws_access_key_id: AWS access key ID (optional if using profile)
            aws_secret_access_key: AWS secret access key (optional if using profile)
            aws_session_token: AWS session token (optional)
            profile_name: AWS profile name (optional)
            **service_params: Additional service-specific parameters
                For Redshift:
                    - cluster_identifier: Redshift cluster identifier
                    - database: Database name
                    - user: Database user
                    - password: Database password
                    - port: Port number (default: 5439)
                For S3:
                    - bucket: Default S3 bucket name
                For DynamoDB:
                    - table: Default table name
        """
        self.service = service.lower()
        self.region = region
        self.service_params = service_params
        
        # Initialize session
        session_kwargs = {
            "region_name": region
        }
        
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs.update({
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key
            })
            if aws_session_token:
                session_kwargs["aws_session_token"] = aws_session_token
        elif profile_name:
            session_kwargs["profile_name"] = profile_name
            
        self.session = boto3.Session(**session_kwargs)
        self._client = None
        self._resource = None
        
        # For Redshift JDBC connection
        if self.service == 'redshift':
            import redshift_connector
            self._redshift_conn = None
            self._redshift_params = {
                "database": service_params.get("database"),
                "user": service_params.get("user"),
                "password": service_params.get("password"),
                "port": service_params.get("port", 5439),
                "cluster_identifier": service_params.get("cluster_identifier")
            }

    def connect(self) -> bool:
        """Establish connection to AWS service."""
        try:
            if self.service == 's3':
                self._client = self.session.client('s3')
                self._resource = self.session.resource('s3')
                return True
                
            elif self.service == 'dynamodb':
                self._client = self.session.client('dynamodb')
                self._resource = self.session.resource('dynamodb')
                return True
                
            elif self.service == 'redshift':
                # Create Redshift client for data API
                self._client = self.session.client('redshift-data')
                
                # Establish JDBC connection for direct access
                import redshift_connector
                self._redshift_conn = redshift_connector.connect(
                    **self._redshift_params
                )
                return True
                
            else:
                raise ValueError(f"Unsupported AWS service: {self.service}")
                
        except Exception as e:
            print(f"Failed to connect to AWS {self.service}: {str(e)}")
            return False

    def read_records(
        self, 
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Read records from AWS service."""
        try:
            if self.service == 's3':
                # Handle S3 path or select query
                if query.lower().startswith('select '):
                    # S3 Select query
                    bucket = params.get('bucket', self.service_params.get('bucket'))
                    key = params.get('key')
                    file_format = params.get('format', 'csv')
                    
                    select_params = {
                        'Bucket': bucket,
                        'Key': key,
                        'Expression': query,
                        'ExpressionType': 'SQL',
                        'InputSerialization': {
                            file_format.upper(): {}
                        },
                        'OutputSerialization': {
                            'JSON': {}
                        }
                    }
                    
                    response = self._client.select_object_content(**select_params)
                    records = []
                    
                    for event in response['Payload']:
                        if 'Records' in event:
                            records.extend([
                                json.loads(line)
                                for line in event['Records']['Payload'].decode('utf-8').splitlines()
                            ])
                            
                    return records
                else:
                    # Direct file read
                    bucket = params.get('bucket', self.service_params.get('bucket'))
                    obj = self._resource.Object(bucket, query)
                    content = obj.get()['Body'].read().decode('utf-8')
                    
                    if query.endswith('.csv'):
                        df = pd.read_csv(StringIO(content))
                        return df.to_dict('records')
                    elif query.endswith('.json'):
                        data = json.loads(content)
                        return data if isinstance(data, list) else [data]
                    else:
                        raise ValueError(f"Unsupported file format: {query}")
                        
            elif self.service == 'dynamodb':
                # Handle DynamoDB operations
                table = self._resource.Table(
                    params.get('table', self.service_params.get('table', query))
                )
                
                if params and 'Key' in params:
                    # Get item by key
                    response = table.get_item(Key=params['Key'])
                    return [response['Item']] if 'Item' in response else []
                else:
                    # Scan table
                    response = table.scan()
                    return response.get('Items', [])
                    
            elif self.service == 'redshift':
                # Execute query using JDBC connection
                cursor = self._redshift_conn.cursor()
                cursor.execute(query, params or {})
                
                columns = [desc[0] for desc in cursor.description]
                results = []
                
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                    
                cursor.close()
                return results
                
        except Exception as e:
            print(f"Error reading from {self.service}: {str(e)}")
            return []

    def write_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str
    ) -> int:
        """Write records to AWS service."""
        if not records:
            return 0
            
        try:
            if self.service == 's3':
                # Write to S3
                bucket = self.service_params.get('bucket')
                if target.endswith('.csv'):
                    df = pd.DataFrame(records)
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)
                    self._client.put_object(
                        Bucket=bucket,
                        Key=target,
                        Body=csv_buffer.getvalue()
                    )
                elif target.endswith('.json'):
                    self._client.put_object(
                        Bucket=bucket,
                        Key=target,
                        Body=json.dumps(records, indent=2)
                    )
                else:
                    raise ValueError(f"Unsupported file format: {target}")
                    
            elif self.service == 'dynamodb':
                # Write to DynamoDB
                table = self._resource.Table(
                    self.service_params.get('table', target)
                )
                
                with table.batch_writer() as batch:
                    for record in records:
                        batch.put_item(Item=record)
                        
            elif self.service == 'redshift':
                # Write to Redshift using COPY command
                columns = list(records[0].keys())
                placeholders = ','.join(['%s' for _ in columns])
                insert_sql = f"INSERT INTO {target} ({','.join(columns)}) VALUES ({placeholders})"
                
                cursor = self._redshift_conn.cursor()
                
                for record in records:
                    values = [record[col] for col in columns]
                    cursor.execute(insert_sql, values)
                    
                self._redshift_conn.commit()
                cursor.close()
                
            return len(records)
            
        except Exception as e:
            print(f"Error writing to {self.service}: {str(e)}")
            if self.service == 'redshift':
                self._redshift_conn.rollback()
            return 0

    def update_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str
    ) -> int:
        """Update records in AWS service."""
        if not records:
            return 0
            
        try:
            if self.service == 'dynamodb':
                # Update DynamoDB items
                table = self._resource.Table(
                    self.service_params.get('table', target)
                )
                count = 0
                
                for record in records:
                    if 'id' not in record:
                        continue
                        
                    update_expr = "SET " + ", ".join([
                        f"#{k} = :{k}"
                        for k in record.keys()
                        if k != 'id'
                    ])
                    
                    expr_names = {
                        f"#{k}": k
                        for k in record.keys()
                        if k != 'id'
                    }
                    
                    expr_values = {
                        f":{k}": v
                        for k, v in record.items()
                        if k != 'id'
                    }
                    
                    table.update_item(
                        Key={'id': record['id']},
                        UpdateExpression=update_expr,
                        ExpressionAttributeNames=expr_names,
                        ExpressionAttributeValues=expr_values
                    )
                    count += 1
                    
                return count
                
            elif self.service == 'redshift':
                # Update Redshift records
                if not records or 'id' not in records[0]:
                    return 0
                    
                cursor = self._redshift_conn.cursor()
                count = 0
                
                for record in records:
                    set_clause = ",".join([
                        f"{k}=%s"
                        for k in record.keys()
                        if k != 'id'
                    ])
                    
                    update_sql = f"UPDATE {target} SET {set_clause} WHERE id=%s"
                    values = [
                        v for k, v in record.items()
                        if k != 'id'
                    ] + [record['id']]
                    
                    cursor.execute(update_sql, values)
                    count += 1
                    
                self._redshift_conn.commit()
                cursor.close()
                return count
                
            else:
                # For S3, we need to read, update, and write back
                existing_records = self.read_records(target)
                if not existing_records:
                    return 0
                    
                updates_by_id = {r['id']: r for r in records if 'id' in r}
                count = 0
                
                for i, record in enumerate(existing_records):
                    if record.get('id') in updates_by_id:
                        existing_records[i].update(updates_by_id[record['id']])
                        count += 1
                        
                if count > 0:
                    self.write_records(existing_records, target)
                    
                return count
                
        except Exception as e:
            print(f"Error updating records in {self.service}: {str(e)}")
            if self.service == 'redshift':
                self._redshift_conn.rollback()
            return 0

    def delete_records(
        self, 
        record_ids: List[str], 
        target: str
    ) -> int:
        """Delete records from AWS service."""
        if not record_ids:
            return 0
            
        try:
            if self.service == 'dynamodb':
                # Delete from DynamoDB
                table = self._resource.Table(
                    self.service_params.get('table', target)
                )
                count = 0
                
                for record_id in record_ids:
                    table.delete_item(Key={'id': record_id})
                    count += 1
                    
                return count
                
            elif self.service == 'redshift':
                # Delete from Redshift
                placeholders = ','.join(['%s' for _ in record_ids])
                delete_sql = f"DELETE FROM {target} WHERE id IN ({placeholders})"
                
                cursor = self._redshift_conn.cursor()
                cursor.execute(delete_sql, record_ids)
                count = cursor.rowcount
                
                self._redshift_conn.commit()
                cursor.close()
                return count
                
            else:
                # For S3, read, filter, and write back
                existing_records = self.read_records(target)
                if not existing_records:
                    return 0
                    
                id_set = set(record_ids)
                new_records = [
                    r for r in existing_records
                    if r.get('id') not in id_set
                ]
                
                if len(new_records) < len(existing_records):
                    self.write_records(new_records, target)
                    return len(existing_records) - len(new_records)
                    
                return 0
                
        except Exception as e:
            print(f"Error deleting records from {self.service}: {str(e)}")
            if self.service == 'redshift':
                self._redshift_conn.rollback()
            return 0

    def close(self) -> None:
        """Close AWS service connections."""
        if self.service == 'redshift' and self._redshift_conn:
            self._redshift_conn.close()
        
        self._client = None
        self._resource = None 