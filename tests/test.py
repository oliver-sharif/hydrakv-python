from hydrakv import client
import asyncio

async def main():
    c = client.Hydrakv(host="localhost", use_grpc=True, grpc_port=9292)
    response = await c.create_db("testdb")
    response = await c.fifolifo_create(name="testqueue", db="testdb", limit=100)
    response = await c.fifolifo_push(name="testqueue", db="testdb", value="test")
    response = await c.fifo_pop(name="testqueue", db="testdb")
    response = await c.fifolifo_delete(name="testqueue", db="testdb")

if __name__ == "__main__":
    asyncio.run(main())


