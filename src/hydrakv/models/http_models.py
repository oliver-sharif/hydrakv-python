from pydantic import BaseModel, Field

# Create a DB
class CreateDB(BaseModel):
    name: str = Field(..., description="The name of the database to create.")

# Set or update a DB
class UpdateDB(BaseModel):
    apikey: str = Field(None, description="The API key for authentication.")
    keys: str = Field(..., description="The keys to set or update.")
    value: str = Field(..., description="The values to set or update.")

# Set a value if not exists
class SetNX(BaseModel):
    ttl: int = Field(None, description="The time-to-live for the key in seconds.")
    key: str = Field(..., description="The key to set.")
    value: str = Field(..., description="The value to set.")
    apikey: str = Field(None, description="The API key for authentication.")

class Set(BaseModel):
    ttl: int = Field(None, description="The time-to-live for the key in seconds.")
    key: str = Field(..., description="The key to set.")
    value: str = Field(..., description="The value to set.")
    apikey: str = Field(None, description="The API key for authentication.")

# Get a value
class Get(BaseModel):
    key: str = Field(..., description="The key to get.")
    apikey: str = Field(None, description="The API key for authentication.")

# Delete a key
class Delete(BaseModel):
    key: str = Field(..., description="The key to delete.")
    apikey: str = Field(None, description="The API key for authentication.")

class Incr(BaseModel):
    key: str = Field(..., description="The key to increment.")
    delta: int = Field(1, description="The amount by which to increment the key's value.")
    apikey: str = Field(None, description="The API key for authentication.")

class FiFoLiFoDelete(BaseModel):
    name: str = Field(..., description="The name of the FiFoLiFo to delete.")

class FiFoLiFoPush(BaseModel):
    name: str = Field(..., description="The name of the FiFoLiFo to push to.")
    value: str = Field(..., description="The value to push.")

class FiFoLiFoPop(BaseModel):
    name: str = Field(..., description="The name of the FiFoLiFo to pop from.")

class FiFoLiFoCreate(BaseModel):
    name: str = Field(..., description="The name of the FiFoLiFo to create.")
    limit: int = Field(..., description="The limit of the FiFoLiFo.")
