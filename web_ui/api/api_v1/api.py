from fastapi import APIRouter

from .endpoints import crawler

api_router = APIRouter()

api_router.include_router(crawler.router)
