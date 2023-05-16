# Import the Canvas class
from functools import wraps
import secrets
import sys
from dotenv import load_dotenv
from os import getenv
from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from loadNotion import getNotion

load_dotenv()  # take environment variables from .env.

app = FastAPI(
    title="Canvas Notion Sync",
    description="Sync Canvas and Notion",
    version="0.1.0",
)
security = HTTPBasic()

def basic_auth(f):
    @wraps(f)
    async def wrapper(credentials: Annotated[HTTPBasicCredentials, Depends(security)], *args, **kwargs):
        current_username_bytes = credentials.username.encode("utf8")
        current_password_bytes = credentials.password.encode("utf8")
        is_correct_username = secrets.compare_digest(
            current_username_bytes, getenv("API_USER").encode("utf8")
        )
        is_correct_password = secrets.compare_digest(
            current_password_bytes, getenv("API_PASS").encode("utf8")
        )
        if not (is_correct_username and is_correct_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Basic"},
            )
        return await f(credentials, *args, **kwargs)
    return wrapper

@app.get("/")
@basic_auth
async def notion(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    return await getNotion()


def receive_signal(signalNumber, frame):
    print('Received:', signalNumber)
    sys.exit()


@app.on_event("startup")
async def startup_event():
    import signal
    signal.signal(signal.SIGINT, receive_signal)
    # startup tasks
