# Flat File Connector

The Flat File Connector enables OpenMatch to read from and write to various flat file formats including CSV, Excel, XML, and JSON files. This connector is particularly useful for data migration, file-based data mastering, and working with local datasets.

## Supported File Formats

- CSV (Comma-Separated Values)
- Excel (.xlsx, .xls)
- XML (eXtensible Markup Language)
- JSON (JavaScript Object Notation)

## Configuration

The FlatFileConnector can be initialized with the following parameters:

```python
from openmatch.connectors import FlatFileConnector

connector = FlatFileConnector(
    base_path="/path/to/data/directory",
    file_type="auto",  # Options: auto, csv, excel, xml, json
    encoding="utf-8",
    csv_separator=",",
    excel_sheet=None  # Optional sheet name for Excel files
)
```

### Parameters

- `base_path` (str): Base directory path for file operations
- `file_type` (str, optional): Type of files to handle. Defaults to "auto"
  - "auto": Automatically detect file type from extension
  - "csv": Force CSV format
  - "excel": Force Excel format
  - "xml": Force XML format
  - "json": Force JSON format
- `encoding` (str, optional): File encoding for text files. Defaults to "utf-8"
- `csv_separator` (str, optional): Separator character for CSV files. Defaults to ","
- `excel_sheet` (str, optional): Sheet name for Excel files. Defaults to None (uses first sheet)

## Usage Examples

### Reading Records

```python
# Read from CSV file
records = connector.read_records("data/customers.csv")

# Read from Excel file with specific sheet
records = connector.read_records("data/sales.xlsx")

# Read from XML file
records = connector.read_records("data/products.xml")

# Read from JSON file
records = connector.read_records("data/orders.json")
```

### Writing Records

```python
# Sample records
records = [
    {"id": "1", "name": "John Doe", "email": "john@example.com"},
    {"id": "2", "name": "Jane Smith", "email": "jane@example.com"}
]

# Write to CSV
connector.write_records(records, "output/customers.csv")

# Write to Excel
connector.write_records(records, "output/customers.xlsx")

# Write to XML
connector.write_records(records, "output/customers.xml")

# Write to JSON
connector.write_records(records, "output/customers.json")
```

### Updating Records

The update operation requires records to have an "id" field for matching:

```python
updates = [
    {"id": "1", "email": "john.doe@example.com"},
    {"id": "2", "email": "jane.smith@example.com"}
]

# Update records in any supported file format
connector.update_records(updates, "data/customers.csv")
```

### Deleting Records

Delete records by their IDs:

```python
record_ids = ["1", "2"]
connector.delete_records(record_ids, "data/customers.csv")
```

## File Format Details

### CSV Files

- Uses pandas for reading and writing
- Supports custom separators
- Handles standard CSV formatting

Example CSV:
```csv
id,name,email
1,John Doe,john@example.com
2,Jane Smith,jane@example.com
```

### Excel Files

- Supports both .xlsx and .xls formats
- Can specify sheet name
- Handles multiple sheets

### XML Files

- Uses ElementTree for parsing
- Expects records in a simple format
- Maintains hierarchical structure

Example XML:
```xml
<?xml version="1.0" encoding="utf-8"?>
<root>
  <record>
    <id>1</id>
    <name>John Doe</name>
    <email>john@example.com</email>
  </record>
  <record>
    <id>2</id>
    <name>Jane Smith</name>
    <email>jane@example.com</email>
  </record>
</root>
```

### JSON Files

- Supports both array and object formats
- Pretty prints output (indented)
- Preserves data types

Example JSON (array format):
```json
[
  {
    "id": "1",
    "name": "John Doe",
    "email": "john@example.com"
  },
  {
    "id": "2",
    "name": "Jane Smith",
    "email": "jane@example.com"
  }
]
```

## Error Handling

The connector includes comprehensive error handling:

- File not found errors
- Permission issues
- Invalid file formats
- Encoding problems
- Data type mismatches

All operations return appropriate status codes and print error messages when issues occur.

## Dependencies

The connector requires the following Python packages:

- pandas: For CSV and Excel file handling
- openpyxl: For Excel file support
- xml.etree.ElementTree: Built-in XML parsing (no additional installation needed)
- json: Built-in JSON handling (no additional installation needed)

## Best Practices

1. Always use appropriate file extensions (.csv, .xlsx, .xml, .json)
2. Ensure consistent data structure across records
3. Include "id" field if update/delete operations are needed
4. Use appropriate encoding for your data
5. Back up important files before operations
6. Close the connector when finished

## Limitations

1. Excel files are limited by pandas/openpyxl capabilities
2. XML files must follow the expected structure
3. Large files may impact performance
4. Binary data should be base64 encoded
5. File locking is not implemented 