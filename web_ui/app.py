# import uvicorn
# from fastapi import FastAPI
# from fastapi.responses import FileResponse
# from fastapi.staticfiles import StaticFiles
# from starlette.middleware.cors import CORSMiddleware

# from config import settings
# from web_ui.api.api_v1.api import api_router
# from pathlib import Path
# import os
# # from job_postings_crawler.config import settings
# # from job_postings_crawler.web_ui.api.api_v1.api import api_router

# app = FastAPI(
#     title=settings.PROJECT_NAME,
#     openapi_url=f"{settings.API_V1_PREFIX}/openapi.json",
#     docs_url=f"{settings.API_V1_PREFIX}/docs",
# )

# static_dir =  "D:\Projects\python\job_postings_crawler\job_postings_crawler\web_ui\static"


# app.mount(
#     "/static",
#     StaticFiles(directory=static_dir),
#     name="static",
# )

# # app.mount(
# #     "/static",
# #     StaticFiles(directory="job_postings_crawler/web_ui/static"),
# #     name="static",
# # )


# if settings.BACKEND_CORS_ORIGINS:
#     app.add_middleware(
#         CORSMiddleware,
#         allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
#         allow_credentials=True,
#         allow_methods=["*"],
#         allow_headers=["*"],
#     )

# app.include_router(api_router, prefix=settings.API_V1_PREFIX)


# @app.get("/", response_class=FileResponse)
# async def read_root():
#     return FileResponse("D:/Projects/python/job_postings_crawler/job_postings_crawler/web_ui/static/index.html")


# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)


import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

from config import settings
from pathlib import Path

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_PREFIX}/openapi.json",
    docs_url=f"{settings.API_V1_PREFIX}/docs",
)

# Configure static file serving
static_dir = "C:/job_postings_crawler/web_ui/static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Configure CORS middleware
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# Import router and include it in the app
from web_ui.api.api_v1.api import api_router
app.include_router(api_router, prefix=settings.API_V1_PREFIX)

# Define your application routes
@app.get("/", response_class=FileResponse)
async def read_root():
    return FileResponse("C:/job_postings_crawler/web_ui/static/index.html")

# Run the application with uvicorn
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
