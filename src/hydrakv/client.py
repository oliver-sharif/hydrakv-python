import sys
from asyncio import run
from typing import Dict, Any

from httpx import Client, AsyncClient
from loguru import logger

import grpc
from .models import hydrakv_pb2_grpc
from .models import hydrakv_pb2

from .models.http_models import *

import json


class Hydrakv:
    """
    Handles communication with a HydraKV server for key-value store operations.

    This class manages client interactions with a HydraKV server. It supports
    both HTTP and gRPC protocols and includes methods for key-value operations,
    such as setting, getting, and deleting entries. Optional HTTPS communication
    is supported for secure interactions. Logging is configurable to provide
    troubleshooting and monitoring capabilities.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 9191, use_grpc: bool = False, grpc_port: int = 9292,
                 grpc_deadline: int = 3, https: bool = False, log_lvl: str = "DEBUG", trusted_cert: str = None,
                 api_key: Dict = None) -> None:
        """
        Initializes the client for connecting to a HydraKV server.

        The constructor sets up the basic connection parameters such as host, port,
        protocol type (HTTP or HTTPS), and whether gRPC is used. It also configures
        the logging mechanism and validates the connection to ensure the server can
        be reached. If the server is unreachable or improperly configured, an exception
        will be raised.

        Attributes:
            log_lvl: Specifies the logging level.

        Parameters:
            host: The hostname or IP address of the HydraKV server.
            port: The port number of the HydraKV server. Defaults to 9191 unless using gRPC
                  with a non-standard port.
            use_grpc: Specifies whether to use gRPC for communication. Defaults to False.
            grpc_port: The port number for gRPC communication. Defaults to 9292.
            grpc_deadline: The deadline for gRPC operations in seconds. Defaults to 3.
            https: Enables or disables HTTPS for secure communication. Defaults to False.
            log_lvl: String denoting the logging level, e.g., "DEBUG", "INFO".
            trusted_cert: Path to the trusted certificate file for HTTPS connections.
            api_key: The API key for authentication. e.g {"dbname": "apikey", ...}

        Raises:
            Exception: Raised if the client cannot connect to the HydraKV server or if the
                       server configuration is invalid.
        """

        # set connection parameters
        self._use_grpc = use_grpc
        self._port = port
        self._grpc_port = 9292
        self._host = host
        self._client: AsyncClient = None
        self.log_lvl = log_lvl
        self._logger = logger
        self._https = https
        self._trusted_cert = trusted_cert
        self._channel = None
        self._stub = None
        self._grpc_deadline = grpc_deadline
        self._apikeys = None

        # configure logger
        self._configure_logger()

        # set port if using gRPC or a non-standard port
        if self._use_grpc:
            logger.debug(f"Using gRPC on port {grpc_port}")
            self._grpc_port = grpc_port

        # set api keys
        if isinstance(api_key, dict):
            self._apikeys = api_key
        else:
            self._apikeys = {}

        # set the protocol
        self._set_protocol()

        # Ok lets Check if we can connect to the server
        try:
            resp = self._chk_connection()
            if "exists" not in resp.keys():
                raise Exception("Could not connect to HydraKV Server - please check IP / Port")
            else:
                print("Connected to HydraKV Server")
        except SystemExit:
            logger.warning("HydraKV Server is not reachable at start, but client is initialized.")

    def _set_protocol(self) -> None:
        """
        Sets the communication protocol (HTTP/HTTPS) and initializes the appropriate
        channel or string identifier for transport, depending on the provided settings.

        This method configures either a secure or insecure channel for gRPC, or a URL
        prefix for HTTP communications. The choice of protocol depends on
        whether HTTPS or HTTP is enabled in the configuration. A warning is logged to
        indicate the use of either option, especially highlighting the insecurity of
        using HTTP for production use.

        :raises Warning: Logs a warning for security-related context with protocol selection.
        """

        # set protocol
        if self._https:
            self._logger.warning("Using HTTPS")
            if self._use_grpc:
                self._channel = grpc.secure_channel(f"{self._host}:{self._grpc_port}",
                                                    grpc.ssl_channel_credentials(self._get_trusted_cert()))
                self._stub = hydrakv_pb2_grpc.KVServiceStub(self._channel)

            # set the http string
            self._http_str = "https://"
        else:
            self._logger.warning("Using HTTP - not recommended for production use! HTTP is insecure.")
            if self._use_grpc:
                self._channel = grpc.insecure_channel(f"{self._host}:{self._grpc_port}")
                self._stub = hydrakv_pb2_grpc.KVServiceStub(self._channel)

            # set the http string
            self._http_str = "http://"

    def _get_trusted_cert(self) -> str:
        """
        Retrieves the trusted certificate from the provided file path.

        :return: The content of the trusted certificate file.
        """
        with open(self._trusted_cert, "r") as f:
            return f.read()

    def _configure_logger(self) -> None:
        """
        Configures the logger with specific settings for output and format.

        This method removes any existing logger configurations and adds a new
        configuration that outputs logs to the standard output stream. The log level
        is dynamically set based on the provided configuration, and the format is
        customized to include a timestamp, log level, logger name, function name,
        line number, and the log message.

        Raises:
            None
        """
        # configure logger
        logger.remove()
        logger.add(
            sys.stdout,
            level=self.log_lvl.upper(),
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                   "<level>{message}</level>",
        )

    def _get_client(self) -> AsyncClient:
        """
        Returns an instance of AsyncClient.

        This method checks if the internal AsyncClient instance is already created.
        If it is not, a new instance is created and returned.

        Returns:
            AsyncClient: An instance of AsyncClient.
        """
        if self._client is None:
            self._logger.debug("Creating new AsyncClient")
            self._client = AsyncClient()
        return self._client

    def _chk_connection(self) -> Dict:
        """
        Checks the connection to the server by sending a GET request to a random,
        non-existent database endpoint. This is used to confirm if the server responds
        as expected.

        Returns:
            Dict: The JSON response from the server indicating the connection status.
        """
        # Check if we have a connection - we will GET a random DB - lets see if the Server responses
        try:
            with Client() as client:
                self._logger.debug("Checking Connection")
                response = client.get(f"{self._http_str}{self._host}:{self._port}/db/random4223423")
                self._logger.debug(response.json())
            return response.json()
        except Exception as e:
            self._logger.error("Not connected to Server: " + str(e))
            exit(1)

    async def set(self, db: str, key: str, value: str, ttl: int = 0, api_key: str = None) -> int | Any:
        """
        Asynchronously sets a value for a specified key in the database.

        This method sends a request to set the value associated with a specific key
        in a targeted database. The method supports both gRPC and HTTP protocols for
        communication. In case of HTTP, it utilizes an asynchronous HTTP client to
        send the request.

        Parameters:
        db (str): The name of the database where the key-value pair should be set.
        key (str): The key to associate with the provided value.
        value (str): The value to set for the specified key.
        ttl (int, optional): The time-to-live for the key in seconds. Defaults to 0, indicating no expiration.
        api_key (str, optional): The API key for authentication. If not provided, it will be looked up in the instance's API keys.

        Returns:
        int: The status code indicating the result of the operation. If gRPC is
             used, the method always returns 0.
        """
        # use api key if provided, else use the one from the instance
        if api_key is None:
            api_key = self._apikeys.get(db, "")

        # create the model for the request
        sr = Set(key=key, apikey=api_key, value=value, ttl=ttl)

        if self._use_grpc:
            try:
                self._logger.debug("using GRPC SET")
                request = hydrakv_pb2.SetRequest(db=db, apikey=sr.apikey, key=sr.key, value=sr.value, ttl=sr.ttl)
                response = self._stub.Set(request, timeout=self._grpc_deadline)
                return response
            except grpc.RpcError as e:
                self._logger.error(f"GRPC SET failed: {e}")
                raise
        else:
            try:
                self._logger.debug("using HTTP SET")
                client = self._get_client()
                headers = {"X-API-Key": api_key} if api_key else {}
                self._logger.debug(f"sending PUT to {self._http_str}{self._host}:{self._port}/db/{db}")
                resp = await client.put(f"{self._http_str}{self._host}:{self._port}/db/{db}", json=sr.model_dump(),
                                        headers=headers)
                return resp.status_code
            except Exception as e:
                self._logger.error(f"HTTP SET failed: {e}")
                raise

    async def get(self, db: str, key: str, api_key: str = None) -> str:
        """
        Asynchronous method for retrieving the value associated with a specified key from a database.

        This method handles the retrieval of a key's value from a database using either gRPC or HTTP protocols, based on
        the configuration. It ensures compatibility with both protocols and performs the required request using the
        appropriate client.

        Parameters:
            db (str): The name of the database from which to retrieve the key's value.
            key (str): The key whose associated value is being requested.
            api_key (str, optional): The API key for authentication. If not provided, it will be looked up in the instance's API keys.

        Returns:
            str: The value associated with the provided key.

        Raises:
            Any exceptions raised during the HTTP or gRPC request process are propagated to the caller.
        """
        # use api key if provided, else use the one from the instance
        if api_key is None:
            api_key = self._apikeys.get(db, "")

        # create the model for the request
        gr = Get(key=key, apikey=api_key)

        if self._use_grpc:
            try:
                self._logger.debug("using GRPC GET")
                request = hydrakv_pb2.GetRequest(db=db, apikey=gr.apikey, key=key)
                response = self._stub.Get(request, timeout=self._grpc_deadline)
                return response.value
            except grpc.RpcError as e:
                self._logger.error(f"GRPC GET failed: {e}")
                raise
        else:
            try:
                self._logger.debug("using HTTP GET")
                client = self._get_client()
                headers = {"X-API-Key": api_key} if api_key else {}
                self._logger.debug(f"sending POST to {self._http_str}{self._host}:{self._port}/db/{db}/keys")
                resp = await client.post(f"{self._http_str}{self._host}:{self._port}/db/{db}/keys", json=gr.model_dump(),
                                         headers=headers)
                return resp.json()["value"]
            except Exception as e:
                self._logger.error(f"HTTP GET failed: {e}")
                raise

    async def setnx(self, db: str, key: str, value: str, ttl: int = 0, api_key: str = None) -> int:
        """
        Sets a value to a specified key in a database only if the key does not already exist.

        Parameters:
            db (str): The name of the database where the key-value pair will be stored.
            key (str): The unique key to set the value for.
            value (str): The value to associate with the specified key.
            ttl (int, optional): The time-to-live for the key in seconds. Defaults to 0, indicating no expiration.
            api_key (str, optional): The API key for authentication. If not provided, it will be looked up in the instance's API keys.

        Returns:
            int: Returns 1 if the key was set successfully, otherwise 0 if the key already exists.
        """
        # use api key if provided, else use the one from the instance
        if api_key is None:
            api_key = self._apikeys.get(db, "")

        sr = SetNX(key=key, value=value, ttl=ttl, apikey=api_key)

        if self._use_grpc:
            try:
                self._logger.debug("using GRPC SETNX")
                request = hydrakv_pb2.SetRequest(db=db, apikey=sr.apikey, key=sr.key, value=sr.value, ttl=sr.ttl)
                response = self._stub.SetNX(request, timeout=self._grpc_deadline)
                return response.ok
            except grpc.RpcError as e:
                self._logger.error(f"GRPC SETNX failed: {e}")
                raise
        else:
            try:
                self._logger.debug("using HTTP SETNX")
                client = self._get_client()
                headers = {"X-API-Key": api_key} if api_key else {}
                self._logger.debug(f"sending POST to {self._http_str}{self._host}:{self._port}/db/{db}")
                resp = await client.post(f"{self._http_str}{self._host}:{self._port}/db/{db}", json=sr.model_dump(),
                                         headers=headers)
                return resp.status_code
            except Exception as e:
                self._logger.error(f"HTTP SETNX failed: {e}")
                raise

    async def incr(self, db: str, key: str, delta: int = 1, api_key: str = None) -> int:
        """
        Increments the value of a specified key by a given delta.

        Parameters:
            db (str): The name of the database where the key-value pair is stored.
            key (str): The key whose value should be incremented.
            delta (int, optional): The amount by which the key's value should be incremented. Defaults to 1.
            api_key (str, optional): The API key for authentication. If not provided, it will be looked up in the instance's API keys.

        Returns:
            int: The new value of the key after incrementation.

        """
        # use api key if provided, else use the one from the instance
        if api_key is None:
            api_key = self._apikeys.get(db, "")

        ir = Incr(key=key, apikey=api_key, delta=delta)

        if self._use_grpc:
            try:
                self._logger.debug("using GRPC INCR")
                request = hydrakv_pb2.IncrRequest(db=db, apikey=ir.apikey, key=ir.key, amount=str(ir.delta))
                response = self._stub.Incr(request, timeout=self._grpc_deadline)
                return response.ok
            except grpc.RpcError as e:
                self._logger.error(f"GRPC INCR failed: {e}")
                raise
        else:
            try:
                self._logger.debug("using HTTP INCR")
                client = self._get_client()
                headers = {"X-API-Key": api_key} if api_key else {}
                resp = await client.request("PATCH", f"{self._http_str}{self._host}:{self._port}/db/{db}",
                                            json=ir.model_dump(), headers=headers)
                return resp.status_code
            except Exception as e:
                self._logger.error(f"HTTP INCR failed: {e}")
                raise

    async def delete(self, db: str, key: str, api_key: str = None) -> Any:
        """
        Deletes a key-value pair from the specified database using either GRPC or HTTP protocol, depending on the instance configuration.

        Parameters:
        db (str): The name of the database from which the key-value pair will be deleted.
        key (str): The key of the key-value pair to be deleted.
        api_key (str, optional): An optional API key to use for the operation. If not provided, the instance's configured API key for the given database will be used.

        Returns:
        Any: The response from the delete operation, which can vary depending on the protocol used (GRPC or HTTP).
        """
        # use api key if provided, else use the one from the instance here
        if api_key is None:
            api_key = self._apikeys.get(db, "")

        dr = Delete(key=key, apikey=api_key)

        if self._use_grpc:
            try:
                self._logger.debug("using GRPC DELETE")
                request = hydrakv_pb2.DeleteRequest(db=db, apikey=dr.apikey, key=dr.key)
                response = self._stub.Delete(request, timeout=self._grpc_deadline)
                return response
            except grpc.RpcError as e:
                self._logger.error(f"GRPC DELETE failed: {e}")
                raise
        else:
            try:
                self._logger.debug("using HTTP DELETE")
                client = self._get_client()
                headers = {"X-API-Key": api_key} if api_key else {}
                self._logger.debug(f"sending DELETE to {self._http_str}{self._host}:{self._port}/db/{db}/{key}")
                resp = await client.request("DELETE", f"{self._http_str}{self._host}:{self._port}/db/{db}/keys",
                                            json=dr.model_dump(), headers=headers)
                return resp.status_code
            except Exception as e:
                self._logger.error(f"HTTP DELETE failed: {e}")
                raise

    async def delete_db(self, name: str = "", api_key: str = None) -> int:
        """
        Deletes a database with the specified name.

        This asynchronous method deletes a database by communicating with the server
        using either gRPC or HTTP, depending on the configuration of the instance.

        Parameters:
        name (str): The name of the database to be deleted. Defaults to an empty string.
        api_key (str, optional): The API key for authentication. If not provided, it will be looked up in the instance's API keys.

        Returns:
        int: The status code of the server response indicating the result of the operation.
        """
        # use api key if provided, else use the one from the instance
        if api_key is None:
            api_key = self._apikeys.get(name, "")

        try:
            client = self._get_client()
            headers = {"X-API-Key": api_key} if api_key else {}
            resp = await client.delete(f"{self._http_str}{self._host}:{self._port}/db/{name}", headers=headers)
            self._logger.debug("Deleted DB: " + name)
            return resp.status_code
        except Exception as e:
            self._logger.error(f"Failed to delete DB: {name}, error: {e}")
            raise

    def get_api_key_for_db(self, db: str) -> str:
        """
        Retrieves the API key associated with a specific database.

        Parameters:
        db (str): The name of the database for which to retrieve the API key.

        Returns:
        str: The API key for the specified database, or an empty string if not found.
        """
        return self._apikeys.get(db, "")

    def get_api_key_as_json(self) -> None:
        """
        Saves the API keys for all databases in a JSON file.

        Returns:
        None: This function does not return a value, it saves the keys to a file.
        """
        with open('api_keys.json', 'w') as f:
            json.dump(self._apikeys, f)

    async def renew_api_key_for_db(self, db: str) -> str:
        """
        Lets the server generate a new API key for a specific database and save / return it.

        Parameters:
            db (str): The name of the database for which to renew the API key.

        Returns:
            str: The newly generated API key for the specified database.
        """

        try:
            client = self._get_client()
            resp = await client.request("UPDATE", f"{self._http_str}{self._host}:{self._port}/db/{db}",
                                        headers={"X-API-Key": self._apikeys.get(db, "")})

            if resp.status_code == 503:
                raise Exception(f"Server currently not using API_KEY auth for DB: {db}")
                return ""

            self._apikeys[db] = resp.json()["apikey"]
            return resp.json()["apikey"]
        except Exception as e:
            self._logger.error(f"Error renewing API key for DB: {db} - {e}")
            return ""

    async def create_db(self, name: str = "") -> int:
        """

        Creates a new database with the specified name asynchronously.

        This method handles the creation of a database using either gRPC or HTTP
        based on the configuration of the instance. If gRPC is enabled, it returns
        a predefined response. Otherwise, it performs an HTTP POST request to
        create the database and returns the status code of the response.

        Parameters:
            name (str): The name of the database to be created. Defaults to an
            empty string.

        Returns:
            int: The status code indicating the success or failure of the creation
            request.
        """
        try:
            cd = CreateDB(name=name)
            client = self._get_client()
            resp = await client.post(f"{self._http_str}{self._host}:{self._port}/create", json=cd.model_dump())
        except Exception as e:
            self._logger.error(f"Failed to create DB: {name}, error: {e}")

        # save new api key if present
        if "apikey" in resp.json().keys():
            self._apikeys[name] = resp.json()["apikey"]

        self._logger.debug("Created DB: " + name)
        return resp.status_code

    async def fifolifo_delete(self, name: str) -> int | Any:
        """
        Deletes a FiFoLiFo by name.

        Parameters:
        name (str): The name of the FiFoLiFo to delete.

        Returns:
        int | Any: The status code or gRPC response.
        """
        ffr = FiFoLiFoDelete(name=name)

        if self._use_grpc:
            try:
                self._logger.debug("using GRPC FIFOLIFO DELETE")
                request = hydrakv_pb2.FiFoLiFoDeleteRequest(name=ffr.name)
                response = self._stub.FiFoLiFoDelete(request, timeout=self._grpc_deadline)
                return response
            except grpc.RpcError as e:
                self._logger.error(f"GRPC FIFOLIFO DELETE failed: {e}")
                raise
        else:
            try:
                self._logger.debug("using HTTP FIFOLIFO DELETE")
                client = self._get_client()
                resp = await client.request("DELETE", f"{self._http_str}{self._host}:{self._port}/fifolifo",
                                            json=ffr.model_dump())
                return resp.status_code
            except Exception as e:
                self._logger.error(f"HTTP FIFOLIFO DELETE failed: {e}")
                raise

    async def fifolifo_push(self, name: str, value: str) -> int | Any:
        """
        Pushes a value to a FiFoLiFo.

        Parameters:
        name (str): The name of the FiFoLiFo.
        value (str): The value to push.

        Returns:
        int | Any: The status code or gRPC response.
        """
        ffp = FiFoLiFoPush(name=name, value=value)

        if self._use_grpc:
            try:
                self._logger.debug("using GRPC FIFOLIFO PUSH")
                request = hydrakv_pb2.FiFoLiFoPushRequest(name=ffp.name, value=ffp.value)
                response = self._stub.FiFoLiFoPush(request, timeout=self._grpc_deadline)
                return response
            except grpc.RpcError as e:
                self._logger.error(f"GRPC FIFOLIFO PUSH failed: {e}")
                raise
        else:
            try:
                self._logger.debug("using HTTP FIFOLIFO PUSH")
                client = self._get_client()
                resp = await client.put(f"{self._http_str}{self._host}:{self._port}/fifolifo",
                                        json=ffp.model_dump())
                return resp.status_code
            except Exception as e:
                self._logger.error(f"HTTP FIFOLIFO PUSH failed: {e}")
                raise

    async def fifo_pop(self, name: str) -> str:
        """
        Pops a value from a FiFo.

        Parameters:
        name (str): The name of the FiFo.

        Returns:
        str: The popped value.
        """
        ffp = FiFoLiFoPop(name=name)

        if self._use_grpc:
            try:
                self._logger.debug("using GRPC FIFO POP")
                request = hydrakv_pb2.FiFoLiFoPopRequest(name=ffp.name)
                response = self._stub.FiFoLiFoFPop(request, timeout=self._grpc_deadline)
                return response.value
            except grpc.RpcError as e:
                self._logger.error(f"GRPC FIFO POP failed: {e}")
                raise
        else:
            try:
                self._logger.debug("using HTTP FIFO POP")
                client = self._get_client()
                resp = await client.post(f"{self._http_str}{self._host}:{self._port}/fifo",
                                         json=ffp.model_dump())
                return resp.json().get("value", "")
            except Exception as e:
                self._logger.error(f"HTTP FIFO POP failed: {e}")
                raise

    async def lifo_pop(self, name: str) -> str:
        """
        Pops a value from a LiFo.

        Parameters:
        name (str): The name of the LiFo.

        Returns:
        str: The popped value.
        """
        ffp = FiFoLiFoPop(name=name)

        if self._use_grpc:
            try:
                self._logger.debug("using GRPC LIFO POP")
                request = hydrakv_pb2.FiFoLiFoPopRequest(name=ffp.name)
                response = self._stub.FiFoLiFoLPop(request, timeout=self._grpc_deadline)
                return response.value
            except grpc.RpcError as e:
                self._logger.error(f"GRPC LIFO POP failed: {e}")
                raise
        else:
            try:
                self._logger.debug("using HTTP LIFO POP")
                client = self._get_client()
                resp = await client.post(f"{self._http_str}{self._host}:{self._port}/lifo",
                                         json=ffp.model_dump())
                return resp.json().get("value", "")
            except Exception as e:
                self._logger.error(f"HTTP LIFO POP failed: {e}")
                raise

    async def fifolifo_create(self, name: str, limit: int) -> int:
        """
        Creates a new FiFoLiFo.
        Note: This operation is only supported via HTTP.

        Parameters:
        name (str): The name of the FiFoLiFo.
        limit (int): The limit of the FiFoLiFo.

        Returns:
        int: The status code of the response.
        """
        ffc = FiFoLiFoCreate(name=name, limit=limit)

        try:
            self._logger.debug("using HTTP FIFOLIFO CREATE")
            client = self._get_client()
            resp = await client.post(f"{self._http_str}{self._host}:{self._port}/fifolifo",
                                     json=ffc.model_dump())
            return resp.status_code
        except Exception as e:
            self._logger.error(f"HTTP FIFOLIFO CREATE failed: {e}")
            raise


async def main():
    hc = Hydrakv("127.0.0.1", 9191, grpc_port=9292, https=False, log_lvl="DEBUG", use_grpc=False)
    # create db
    await hc.create_db("test")
    # set key
    print("Cool, ", await hc.set("test", "key", "value", ttl=5))
    # get key
    print("Cool, ", await hc.get("test", "key"))
    # delete key
    print("Cool, ", await hc.delete("test", "key"))
    # delete db
    print("Cool, ", await hc.delete_db("test"))


if __name__ == "__main__":
    run(main())
