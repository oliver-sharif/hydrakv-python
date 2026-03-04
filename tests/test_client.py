import asyncio
import sys
import os


# to run: python3 tests/test_client.py


# Ensure local src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from hydrakv.client import Hydrakv

async def run_basic_tests(client, db_name):
    print(f"  Testing basic operations on {db_name}...")
    target_db = db_name
    
    # Set and Get
    print(f"    Testing set/get...")
    try:
        await client.set(db=target_db, key="test_key", value="test_value")
        val = await client.get(db=target_db, key="test_key")
        print(f"      Got value: {val}")
    except Exception as e:
        print(f"    Set/Get failed: {e}")
    
    # SetNX
    print(f"    Testing setnx...")
    try:
        await client.setnx(db=target_db, key="nx_key", value="nx_value")
        val = await client.get(db=target_db, key="nx_key")
        print(f"      Got NX value: {val}")
    except Exception as e:
        print(f"    SetNX failed: {e}")
    
    # Incr
    print(f"    Testing incr...")
    try:
        await client.set(db=target_db, key="counter", value="10")
        await client.incr(db=target_db, key="counter", delta=5)
        val = await client.get(db=target_db, key="counter")
        print(f"      Got Incr value: {val}")
    except Exception as e:
        print(f"    Incr failed: {e}")
    
    # Delete key
    print(f"    Testing delete...")
    try:
        await client.delete(db=target_db, key="test_key")
        val = await client.get(db=target_db, key="test_key")
        print(f"      After delete: {val}")
    except Exception as e:
        print(f"    Delete failed: {e}")
    
    print(f"  Basic operations on {target_db} finished.")

async def run_fifolifo_tests(client, db_name="default", name="test_queue"):
    print(f"  Testing FiFo/LiFo operations on {name} in db {db_name}...")
    try:
        # Create - FiFoLiFos are created via HTTP (even if client uses gRPC for other things)
        print(f"    Creating {name}...")
        await client.fifolifo_create(name=name, db=db_name, limit=10)
        
        # Give the server a moment to synchronize if needed
        await asyncio.sleep(0.1)
        
        # Push
        print(f"    Pushing to {name}...")
        await client.fifolifo_push(name=name, db=db_name, value="first")
        await client.fifolifo_push(name=name, db=db_name, value="second")
        
        # FIFO Pop (First In First Out -> "first")
        val = await client.fifo_pop(name=name, db=db_name)
        print(f"      FIFO Pop: {val}")
        
        # LIFO Pop (Last In First Out -> "second" since only it remains or if we push more)
        await client.fifolifo_push(name=name, db=db_name, value="third")
        val = await client.lifo_pop(name=name, db=db_name)
        print(f"      LIFO Pop: {val}")
        
        # Delete
        print(f"    Deleting {name}...")
        await client.fifolifo_delete(name=name, db=db_name)
        print(f"  FiFo/LiFo operations on {name} passed.")
    except Exception as e:
        print(f"  FiFo/LiFo failed: {e}")

async def main():
    # Setup - Ensure database exists once
    setup_client = Hydrakv(host="127.0.0.1", port=9191, use_grpc=False)
    print(f"Ensuring database 'default' exists...")
    try:
        await setup_client.create_db(name="default")
    except Exception as e:
        print(f"Note: create_db failed (may already exist): {e}")

    # HTTP Tests
    print("Starting HTTP tests...")
    http_client = Hydrakv(host="127.0.0.1", port=9191, use_grpc=False)
    await run_basic_tests(http_client, "default")
    await run_fifolifo_tests(http_client, "default", "http_test_queue")
    print("HTTP tests finished!\n")

    # gRPC Tests
    print("Starting gRPC tests...")
    grpc_client = Hydrakv(host="127.0.0.1", grpc_port=9292, use_grpc=True)
    await run_basic_tests(grpc_client, "default")
    
    # Mixed test for FiFo/LiFo
    try:
        print("Starting gRPC FiFo/LiFo tests...")
        queue_name = "grpc_test_queue"
        await run_fifolifo_tests(grpc_client, "default", queue_name)
    except Exception as e:
        print(f"gRPC FiFo/LiFo failed: {e}")
    
    print("gRPC tests finished!")

if __name__ == "__main__":
    asyncio.run(main())
