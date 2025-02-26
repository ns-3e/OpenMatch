from typing import List, Dict, Any, Optional
import pandas as pd
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from . import Connector

class FlatFileConnector(Connector):
    """Connector for flat files (CSV, Excel, XML, JSON)."""
    
    def __init__(
        self,
        base_path: str,
        file_type: str = "auto",  # auto, csv, excel, xml, json
        encoding: str = "utf-8",
        csv_separator: str = ",",
        excel_sheet: Optional[str] = None
    ):
        """
        Initialize FlatFileConnector.
        
        Args:
            base_path: Base directory path for file operations
            file_type: Type of files to handle (auto, csv, excel, xml, json)
            encoding: File encoding for text files
            csv_separator: Separator character for CSV files
            excel_sheet: Sheet name for Excel files (None for first sheet)
        """
        self.base_path = Path(base_path)
        self.file_type = file_type.lower()
        self.encoding = encoding
        self.csv_separator = csv_separator
        self.excel_sheet = excel_sheet
        self._connected = False

    def connect(self) -> bool:
        """Verify base path exists and is accessible."""
        try:
            if not self.base_path.exists():
                self.base_path.mkdir(parents=True)
            self._connected = True
            return True
        except Exception as e:
            print(f"Failed to access base path: {str(e)}")
            return False

    def _detect_file_type(self, file_path: str) -> str:
        """Detect file type from extension if set to auto."""
        if self.file_type != "auto":
            return self.file_type
            
        ext = Path(file_path).suffix.lower()
        if ext in ['.csv']:
            return 'csv'
        elif ext in ['.xlsx', '.xls']:
            return 'excel'
        elif ext in ['.xml']:
            return 'xml'
        elif ext in ['.json']:
            return 'json'
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

    def read_records(
        self, 
        query: str,  # In this context, query is the relative file path
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Read records from a file."""
        if not self._connected:
            return []
            
        try:
            file_path = self.base_path / query
            file_type = self._detect_file_type(query)
            
            if file_type == 'csv':
                df = pd.read_csv(
                    file_path,
                    sep=self.csv_separator,
                    encoding=self.encoding
                )
                return df.to_dict('records')
                
            elif file_type == 'excel':
                df = pd.read_excel(
                    file_path,
                    sheet_name=self.excel_sheet
                )
                return df.to_dict('records')
                
            elif file_type == 'xml':
                tree = ET.parse(file_path)
                root = tree.getroot()
                records = []
                
                # Convert XML to list of dictionaries
                for child in root:
                    record = {}
                    for elem in child:
                        record[elem.tag] = elem.text
                    records.append(record)
                    
                return records
                
            elif file_type == 'json':
                with open(file_path, 'r', encoding=self.encoding) as f:
                    data = json.load(f)
                    
                # Handle both array and object JSON formats
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    return [data]
                    
            return []
            
        except Exception as e:
            print(f"Error reading file {query}: {str(e)}")
            return []

    def write_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str  # Relative file path
    ) -> int:
        """Write records to a file."""
        if not records or not self._connected:
            return 0
            
        try:
            file_path = self.base_path / target
            file_type = self._detect_file_type(target)
            
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            if file_type == 'csv':
                df = pd.DataFrame(records)
                df.to_csv(
                    file_path,
                    sep=self.csv_separator,
                    encoding=self.encoding,
                    index=False
                )
                
            elif file_type == 'excel':
                df = pd.DataFrame(records)
                df.to_excel(
                    file_path,
                    sheet_name=self.excel_sheet or 'Sheet1',
                    index=False
                )
                
            elif file_type == 'xml':
                root = ET.Element('root')
                for record in records:
                    record_elem = ET.SubElement(root, 'record')
                    for key, value in record.items():
                        field = ET.SubElement(record_elem, str(key))
                        field.text = str(value)
                        
                tree = ET.ElementTree(root)
                tree.write(
                    file_path,
                    encoding=self.encoding,
                    xml_declaration=True
                )
                
            elif file_type == 'json':
                with open(file_path, 'w', encoding=self.encoding) as f:
                    if len(records) == 1:
                        json.dump(records[0], f, indent=2)
                    else:
                        json.dump(records, f, indent=2)
                        
            return len(records)
            
        except Exception as e:
            print(f"Error writing to file {target}: {str(e)}")
            return 0

    def update_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str
    ) -> int:
        """Update records in a file based on ID field."""
        if not records or not self._connected or "id" not in records[0]:
            return 0
            
        try:
            # Read existing records
            existing_records = self.read_records(target)
            if not existing_records:
                return 0
                
            # Create ID lookup for faster updates
            updates_by_id = {r["id"]: r for r in records}
            
            # Update matching records
            count = 0
            for i, record in enumerate(existing_records):
                if record.get("id") in updates_by_id:
                    existing_records[i].update(updates_by_id[record["id"]])
                    count += 1
                    
            # Write back all records
            if count > 0:
                self.write_records(existing_records, target)
                
            return count
            
        except Exception as e:
            print(f"Error updating records in {target}: {str(e)}")
            return 0

    def delete_records(
        self, 
        record_ids: List[str], 
        target: str
    ) -> int:
        """Delete records from a file based on ID field."""
        if not record_ids or not self._connected:
            return 0
            
        try:
            # Read existing records
            existing_records = self.read_records(target)
            if not existing_records:
                return 0
                
            # Filter out records to delete
            id_set = set(record_ids)
            new_records = [r for r in existing_records if r.get("id") not in id_set]
            
            # Write back remaining records
            if len(new_records) < len(existing_records):
                self.write_records(new_records, target)
                return len(existing_records) - len(new_records)
                
            return 0
            
        except Exception as e:
            print(f"Error deleting records from {target}: {str(e)}")
            return 0

    def close(self) -> None:
        """Clean up any resources."""
        self._connected = False 