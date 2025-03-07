from fastapi import FastAPI
from schema.schema import TopLevelSchema
from helpers.processor import Processor, Buffer

import config.logger as logger
import config.config as config
import os


logger = logger.setup_logger()
conf = getattr(config, f'{os.environ["APP_ENV"].title()}Config')

app = FastAPI(docs_url=f"/api/{conf.V_API}/docs",
              redoc_url=f"/api/{conf.V_API}/redoc",
              openapi_url=f"/api/{conf.V_API}/openapi.json")

buffer = Buffer()

@app.get(f"/api/{conf.V_API}")
async def root():

    return {"message": f"Welcome to the collector {os.environ['HOSTNAME']}!"}


@app.post(f"/api/{conf.V_API}{conf.COLLECTOR_PATH}")
async def write_data(data: TopLevelSchema):

    try:
        processed_data = Processor(data.dict()).processed_event
        buffer.add(processed_data)
        logger.info(f"Adding event to buffer: {data.dict()}")
    except:
        logger.error(f"Failed to process event: {data}")

    return {"message": f"Event added to buffer"}
