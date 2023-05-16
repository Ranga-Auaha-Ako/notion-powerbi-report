# Create async decorator to cache function results
import asyncio, glom
from diskcache import Cache
from notion.handlers import *
from notion_client.helpers import async_collect_paginated_api

def cacheResult(expire=60*60*11, key=None):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            with Cache("cache") as cache:
                cacheKey = key or f"{func.__name__}({args}, {kwargs})"
                print(f"Cache key: {cacheKey}")
                if cacheKey in cache:
                    return cache[cacheKey]
                else:
                    result = await func(*args, **kwargs)
                    cache.set(cacheKey, result, expire=expire)
                    return result
        return wrapper
    return decorator

notionRelation = lambda target, prop: glom.glom(target, f"properties.{prop}.relation.0.id", default=None)
notionTitle = lambda target, prop: glom.glom(target, f"properties.{prop}.title.0.plain_text", default=None)
notionRichText = lambda target, prop: ",".join(glom.glom(target, f"properties.{prop}.rich_text.*.plain_text", default=[]))
notionSelect = lambda target, prop: glom.glom(target, f"properties.{prop}.select.name", default=None)
notionPeople = lambda target, prop: ",".join(glom.glom(target, glom.Coalesce(f"properties.{prop}.people.*.person.email",f"properties.{prop}.people.*.id", default=[])))
notionNumber = lambda target, prop: glom.glom(target, f"properties.{prop}.number", default=None)
notionStatus = lambda target, prop: glom.glom(target, f"properties.{prop}.status.name", default=None)
notionURL = lambda target, prop: glom.glom(target, f"properties.{prop}.url", default=None)


def transformRows(table, props):
    return {row["id"]: {"id": row["id"],
        **{ prop: g(row, prop) for prop, g in props }}
        for row in table}



@cacheResult(key=f"getNotion:all")
async def getNotion():
    # notionCoursePkgs = await async_collect_paginated_api(getNotionPagesBackoff,database_id=getenv("NOTION_COURSEPKG_DB_ID"), filter={
    #     "property": "Total Students across duplicates",
    #     "rollup": {
    #         "number": {
    #             "greater_than": 1000
    #         }
    #     }
    # })
    # notionTriageTasks = await async_collect_paginated_api(getNotionPagesBackoff,database_id=getenv("NOTION_TRIAGE_DB_ID"), filter={
    #     "property": "Status",
    #     "status": {
    #             "equals": "Triage Complete"
    #     }
    # })
    # notionBaselineTasks = await async_collect_paginated_api(getNotionPagesBackoff,database_id=getenv("NOTION_BASELINE_DB_ID"), filter={
    #     "property": "Status",
    #     "status": {
    #         "equals": "Baseline Complete"
    #     }
    # })
    # notionCourses = await async_collect_paginated_api(getNotionPagesBackoff,database_id=getenv("NOTION_COURSE_DB_ID"), filter={
    #     "property": "Total Students",
    #     "number": {
    #         "greater_than": 1000
    #     }
    # })
    notionCoursePkgs, notionCourses, notionTriageTasks, notionBaselineTasks = await asyncio.gather(
        async_collect_paginated_api(getNotionPagesBackoff,database_id=getenv("NOTION_COURSEPKG_DB_ID")),
        async_collect_paginated_api(getNotionPagesBackoff,database_id=getenv("NOTION_COURSE_DB_ID")),
        async_collect_paginated_api(getNotionPagesBackoff,database_id=getenv("NOTION_TRIAGE_DB_ID")),
        async_collect_paginated_api(getNotionPagesBackoff,database_id=getenv("NOTION_BASELINE_DB_ID")),
    )
    # Transform Course Packages
    coursePkgs = transformRows(notionCoursePkgs, (
        ("Triage Task", notionRelation),
        ("Baseline Task", notionRelation),
        ("Course Name",notionTitle),
        ("Course Description",notionRichText),
        ("Root Account Name",notionSelect),
        ("Account Name",notionSelect),
        ("Primary Component",notionSelect),
        ("Stage",notionSelect),
        ("Subject Code",notionRichText),
        ("Course Package ID",notionRichText),
        ("Course Code",notionRichText)
    ))
    # Transform Triage Tasks
    courseTriageTasks = transformRows(notionTriageTasks, (
        ("Assign",notionPeople),
        ("Status",notionStatus),
        ("Traffic Light for Course",notionSelect),
        ("Requires Attention",notionSelect),
        ("Structure",notionSelect),
        ("Home Page",notionSelect),
        ("Modules",notionSelect),
        ("Pages",notionSelect),
        ("Comments - Structure and Navigation",notionRichText),
        ("Welcome",notionSelect),
        ("Orientation",notionSelect),
        ("Comments - Welcome & Orientation",notionRichText),
        ("Syllabus Page",notionSelect),
        ("Learning Objectives",notionSelect),
        ("Policies & Guidelines",notionSelect),
        ("Comments - Course Syllabus Information",notionRichText),
        ("Overview of Assessment",notionSelect),
        ("Assignment Details",notionSelect),
        ("Points / Weighting",notionSelect),
        ("Due Dates",notionSelect),
        ("Rubrics",notionSelect),
        ("Comments - Assessments",notionRichText),
        ("UDOIT Report",notionSelect),
        ("Reading List (Talis)",notionSelect),
        ("Panopto Video",notionSelect),
        ("Comments - Accessibility & Copyright",notionRichText)
    ))
    # Transform Canvas Courses
    courses = transformRows(notionCourses, (
        ("Course Name",notionTitle),
        ("Course Package",notionRelation),
        ("Canvas Link",notionURL),
        ("DCO Link",notionURL),
        ("Course Status",notionSelect),
        ("Stage",notionSelect),
        ("Term Code",notionSelect),
        ("Total Students",notionNumber),
        ("Course ID",notionNumber)
    ))
    # Transform Baseline Tasks
    baselineTasks = transformRows(notionBaselineTasks, (
        ("Assign",notionPeople),
        ("Status",notionStatus)
    ))
    
    # Merge tables
    merged = [
        {
            **row,
            "Courses": [course for course in courses.values() if course["Course Package"] == row["id"]],
            "Triage Task": courseTriageTasks.get(row["Triage Task"], {}),
            "Baseline Task": baselineTasks.get(row["Baseline Task"], {})
        }
        for row in coursePkgs.values()
    ]

    # Transform to flat structure
    flattened = [
        {
            "id": row["id"],
            "name": row["Course Name"],
            "description": row["Course Description"],
            "rootAccountName": row["Root Account Name"],
            "accountName": row["Account Name"],
            "primaryComponent": row["Primary Component"],
            "stage": row["Stage"],
            "subjectCode": row["Subject Code"],
            "coursePackageId": row["Course Package ID"],
            "courseCode": row["Course Code"],
            **{ f"triage_{prop}": val for prop,val in row["Triage Task"].items() },
            **{ f"baseline_{prop}": val for prop,val in row["Baseline Task"].items() },
            "canvasLinks": ",".join([course["Canvas Link"] for course in row["Courses"]]),
            "dcoLinks": ",".join([course["DCO Link"] for course in row["Courses"]]),
            "termCodes": ",".join([course["Term Code"] for course in row["Courses"]]),
            "totalStudents": sum([course["Total Students"] for course in row["Courses"]]),
        }
        for row in merged
    ]
    return flattened