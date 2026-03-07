# HydraKV Python Client

A Python client for interacting with HydraKV via gRPC or HTTP.

## Installation

### From PyPI

```bash
pip install hydrakv
```

### From Source (for Developers)

If you want to install the package from source (e.g., after cloning the repository), follow these steps:

1. **Clone the repository:**

   ```bash
   git clone https://github.com/oliver-sharif/hydrakv-python.git
   cd hydrakv-python
   ```

2. **Install in Editable Mode:**

   This is recommended if you plan to make changes to the code.

   ```bash
   pip install -e .
   ```

3. **Build the Package:**

   To create a distributable package (wheel and source distribution):

   ```bash
   # Install the build tool if you don't have it
   pip install build

   # Build the package
   python -m build
   ```

   The build artifacts will be located in the `dist/` directory.

4. **Install the Built Package:**

   After building, you can install the `.whl` file:

   ```bash
   pip install dist/hydrakv-0.1.0-py3-none-any.whl
   ```

## Usage

### HTTP Example

```python
import asyncio
from hydrakv import Hydrakv

async def main():
    # Initialize HTTP client (default)
    hc = Hydrakv(host="127.0.0.1", port=9191, use_grpc=False)
    
    # Create a database
    db_name = "example_http"
    print("Create DB:", await hc.create_db(db_name))
    
    # Set a key with TTL (seconds)
    print("Set Key:", await hc.set(db_name, "my_key", "my_value", ttl=60))
    
    # Get a key
    print("Get Key:", await hc.get(db_name, "my_key"))
    
    # Set if not exists (SetNX)
    print("SetNX:", await hc.setnx(db_name, "unique_key", "value"))

    # Increment a counter
    print("Incr:", await hc.incr(db_name, "counter", delta=1))

    # Delete a key
    print("Delete Key:", await hc.delete(db_name, "my_key"))
    
    # Delete the database
    print("Delete DB:", await hc.delete_db(db_name))

if __name__ == "__main__":
    asyncio.run(main())
```

### gRPC Example

```python
import asyncio
from hydrakv import Hydrakv

async def main():
    # Initialize gRPC client
    hc = Hydrakv(host="127.0.0.1", grpc_port=9292, use_grpc=True)
    
    # Create a database
    db_name = "example_grpc"
    print("Create DB:", await hc.create_db(db_name))
    
    # Set a key
    print("Set Key:", await hc.set(db_name, "grpc_key", "grpc_value"))
    
    # Get a key
    print("Get Key:", await hc.get(db_name, "grpc_key"))
    
    # Increment
    print("Incr:", await hc.incr(db_name, "grpc_counter", delta=5))

    # Delete
    print("Delete Key:", await hc.delete(db_name, "grpc_key"))
    
    # Delete DB
    print("Delete DB:", await hc.delete_db(db_name))

if __name__ == "__main__":
    asyncio.run(main())
```

## API Key Management

HydraKV supports API key authentication. The client automatically manages API keys returned during database creation or they can be provided during initialization.

```python
import asyncio
from hydrakv import Hydrakv

async def main():
    hc = Hydrakv(host="127.0.0.1", port=9191)
    
    db_name = "secure_db"
    await hc.create_db(db_name)

    # Get API key for a database (managed automatically after create_db)
    api_key = hc.get_api_key_for_db(db_name)
    print(f"API Key for {db_name}: {api_key}")

    # Renew API key for a database
    new_api_key = await hc.renew_api_key_for_db(db_name)
    print(f"New API Key: {new_api_key}")

    # Save all managed API keys to api_keys.json
    hc.get_api_key_as_json()

if __name__ == "__main__":
    asyncio.run(main())
```

## Client-Side Encryption

HydraKV supports optional client-side encryption for values. This ensures that the server never sees the raw data. The encryption uses RSA key pairs, which are automatically generated and managed by the client.

To enable encryption, provide `value_encryption=True` and a `value_secret` (bytes) to the `Hydrakv` constructor.

```python
import asyncio
from hydrakv import Hydrakv

async def main():
    # Initialize client with encryption
    # A secret key (bytes) is required to protect the local RSA key file
    secret = b"my_super_secret_password"
    hc = Hydrakv(host="127.0.0.1", value_encryption=True, value_secret=secret)
    
    db_name = "encrypted_db"
    await hc.create_db(db_name)
    
    # Values are automatically encrypted before being sent to the server
    await hc.set(db_name, "secret_key", "top_secret_value")
    
    # Values are automatically decrypted when retrieved
    val = await hc.get(db_name, "secret_key")
    print(f"Decrypted value: {val}")

if __name__ == "__main__":
    asyncio.run(main())
```

By default, the RSA keys are stored in `./.keys/<host>.key`. You can specify a custom path using the `value_encryption_path` parameter.

## License

MIT
