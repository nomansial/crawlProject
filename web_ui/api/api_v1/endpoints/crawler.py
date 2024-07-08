import json
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from repository.db_repository import QueueRepository
import repository.db_repository
from repository.models import Crawl
import request_processor.request_processor
from web_ui.api.api_v1.models import CreateCrawl
from web_ui.api.deps import repository
from main import main, stop_event  
import asyncio
import signal
from typing import List
import request_processor

# from job_postings_crawler.repository.db_repository import QueueRepository
# from job_postings_crawler.repository.models import Crawl
# from job_postings_crawler.web_ui.api.api_v1.models import CreateCrawl
# from job_postings_crawler.web_ui.api.deps import repository

router = APIRouter(
    prefix="/crawls",
    tags=["crawl", "crawler"],
    responses={404: {"description": "Not found"}},
)


@router.post("/")
async def schedule_crawl(
    create_crawl: CreateCrawl,
    repository: QueueRepository = Depends(repository),
):
    try:
        # ipdb.set_trace
        if not create_crawl.minimum_date and not create_crawl.include_never_processed:
            raise HTTPException(
                status_code=400,
                detail="Either minimum_date must be set or include_never_processed must be true.",
            )
        success = await repository.schedule_crawl(create_crawl.to_repo_model())
        if not success:
            raise HTTPException(
                status_code=409,
                detail="A crawl is already in progress. Please wait until it finishes first.",
            )
        return {
            "message": "Crawl scheduled successfully",
            "date": create_crawl.minimum_date,
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[Crawl])
async def get_scheduled_crawls(repository: QueueRepository = Depends(repository)):
    try:
        return await repository.get_crawls()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/startCrawl")
async def startCrawl():
    # global stop_event
    stop_event.clear()  # Clear the stop event before starting
    
    asyncio.create_task(main(stop_event))  # Start the main function as a background task
    return json.dumps({"message": "Crawler Started"})


@router.post("/stopCrawl")
async def stopCrawl():
    # global stop_event
    stop_event.set()  # Signal the main function to stop

    return json.dumps({"message": "Crawler Stopped"})


@router.post("/getRecordCount")
async def get_Record_Count(repository: QueueRepository = Depends(repository)):
    count = await repository.count_input_rows()
    response_data = {
        "count": count
    }
    return JSONResponse(content=response_data)


@router.post("/getElapsedTime")
async def get_Elapsed_Time():
    elapsedTime = request_processor.request_processor.elapsed_time
    response_data = {
        "elapsedTime": elapsedTime
    }
    return JSONResponse(content=response_data)



