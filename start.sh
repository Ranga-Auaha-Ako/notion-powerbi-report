#!/bin/bash
(cd src; uvicorn --port 8000 --host 127.0.0.1 main:app --reload)