# OpenMatch Data Model System

The OpenMatch Data Model System provides a Django-like class-based approach for defining and managing your MDM data model. It offers a familiar and intuitive way to define entities, relationships, and fields, including support for vector embeddings and similarity search.

## Table of Contents
1. [Model Definition](#model-definition)
2. [Field Types](#field-types)
3. [Model Options](#model-options)
4. [Relationships](#relationships)
5. [Vector Support](#vector-support)
6. [History and Cross-Reference](#history-and-cross-reference)
7. [Examples](#examples)

## Model Definition

Models are defined using Python classes that inherit from `Model`:

```python
from openmatch.model.models import Model
from openmatch.model.fields import CharField, IntegerField, ForeignKey

class Product(Model):
    id = CharField(max_length=50, primary_key=True)
    name = CharField(max_length=200)
    price = FloatField()
    category = ForeignKey('Category', related_name='products')
    
    class Meta:
        table_name = 'mdm_product'
        history = True  # Enable history tracking
        xref = True    # Enable cross-referencing
```

## Field Types

OpenMatch provides a rich set of field types:

### Basic Fields
```python
# String field
name = CharField(
    max_length=100,
    min_length=1,
    unique=True
)

# Numeric fields
quantity = IntegerField(min_value=0)
price = FloatField(decimal_places=2)

# Boolean field
active = BooleanField(default=True)

# Date/time fields
created_at = DateTimeField(auto_now_add=True)
updated_at = DateTimeField(auto_now=True)

# JSON field
metadata = JSONField(schema={...})
```

### Vector Field
```python
embeddings = VectorField(
    dimensions=384,          # Vector size
    distance_metric='cosine', # 'cosine', 'l2', or 'inner'
    index_type='ivfflat',    # 'ivfflat' or 'hnsw'
    lists=100,               # IVFFlat partitions
    probes=10               # Search probe count
)
```

### Relationship Fields
```python
# Foreign key
category = ForeignKey(
    'Category',
    on_delete='CASCADE',
    related_name='products'
)

# Many-to-many
tags = ManyToManyField(
    'Tag',
    through='ProductTag',
    related_name='products'
)
```

## Model Options

Configure model behavior using the Meta class:

```python
class Meta:
    table_name = 'mdm_product'        # Physical table name
    verbose_name = 'Product'          # Human-readable name
    verbose_name_plural = 'Products'  # Plural form
    history = True                    # Enable history tracking
    xref = True                      # Enable cross-referencing
    
    indexes = [
        {'fields': ['sku'], 'unique': True},
        {'fields': ['category']}
    ]
    
    unique_together = [
        ('name', 'category')
    ]
    
    partition_strategy = {
        'type': 'range',
        'column': 'created_at',
        'interval': 'monthly'
    }
```

## Relationships

### One-to-Many Relationship
```python
class Category(Model):
    name = CharField(max_length=100)

class Product(Model):
    category = ForeignKey(Category, related_name='products')
```

### Many-to-Many Relationship
```python
class Tag(Model):
    name = CharField(max_length=50)
    products = ManyToManyField(Product, through='ProductTag')

class ProductTag(Model):
    product = ForeignKey(Product)
    tag = ForeignKey(Tag)
    added_at = DateTimeField(auto_now_add=True)
```

## Vector Support

OpenMatch provides built-in support for vector embeddings and similarity search:

### Defining Vector Fields
```python
class ProductEmbedding(Model):
    product = ForeignKey(Product, primary_key=True)
    title_embedding = VectorField(
        dimensions=384,
        distance_metric='cosine',
        index_type='ivfflat'
    )
    image_embedding = VectorField(
        dimensions=512,
        distance_metric='l2',
        index_type='hnsw'
    )
```

### Vector Search
```python
# Find similar products
similar_products = await ProductEmbedding.find_similar_products(
    query_vector=embedding,
    field='title_embedding',
    limit=10,
    distance_threshold=0.8
)
```

## History and Cross-Reference

### History Tracking
```python
class ProductHistory(HistoryModel):
    product = ForeignKey(Product, related_name='history')
    data = JSONField()  # Historical state
    
    class Meta:
        indexes = [
            {'fields': ['product', 'valid_from']}
        ]
```

### Cross-Reference
```python
class ProductXref(XrefModel):
    product = ForeignKey(Product, related_name='xrefs')
    
    class Meta:
        indexes = [
            {'fields': ['source_system', 'source_id']}
        ]
```

## Examples

### Complete Model Example
```python
class Product(Model):
    """Product model with history tracking and vector search."""
    
    id = CharField(max_length=50, primary_key=True)
    sku = CharField(max_length=50, unique=True)
    name = CharField(max_length=200)
    description = CharField(max_length=1000, null=True)
    price = FloatField()
    category = ForeignKey(Category, related_name='products')
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    
    class Meta:
        table_name = 'mdm_product'
        verbose_name = 'Product'
        indexes = [
            {'fields': ['sku'], 'unique': True},
            {'fields': ['category']}
        ]
        history = True
        xref = True

class ProductEmbedding(Model):
    """Product embeddings for semantic search."""
    
    product = ForeignKey(Product, primary_key=True)
    title_embedding = VectorField(
        dimensions=384,
        distance_metric='cosine',
        description="BERT embedding of product title"
    )
    image_embedding = VectorField(
        dimensions=512,
        distance_metric='l2',
        description="Vision model embedding"
    )
    
    @classmethod
    async def find_similar_products(cls, query_vector, field='title_embedding'):
        return await cls.vector_search(
            vector=query_vector,
            field=field,
            limit=10
        )
```

### Using the Models

```python
# Create model manager
manager = DataModelManager([
    Category,
    Product,
    ProductEmbedding,
    ProductHistory,
    ProductXref
], engine)

# Create database tables
await manager.create_physical_model()

# Create a product
product = Product(
    id='PROD-001',
    name='Example Product',
    price=99.99,
    category=category
)

# Add embeddings
embedding = ProductEmbedding(
    product=product,
    title_embedding=title_vector,
    image_embedding=image_vector
)

# Find similar products
similar = await ProductEmbedding.find_similar_products(
    query_vector=search_vector
)
```

## Best Practices

1. **Model Organization**
   - Group related models in modules
   - Use clear, descriptive names
   - Add docstrings and comments
   - Follow naming conventions

2. **Field Configuration**
   - Set appropriate field types
   - Use meaningful constraints
   - Configure indexes wisely
   - Document field purpose

3. **Vector Fields**
   - Choose appropriate dimensions
   - Select suitable distance metrics
   - Configure indexes for performance
   - Consider memory usage

4. **Performance**
   - Use appropriate indexes
   - Configure partitioning
   - Optimize vector search
   - Monitor query performance

## Additional Resources

- [OpenMatch Documentation](https://openmatch.readthedocs.io/)
- [API Reference](https://openmatch.readthedocs.io/api/)
- [Examples Repository](https://github.com/openmatch/examples)
- [Community Forum](https://discuss.openmatch.io/) 