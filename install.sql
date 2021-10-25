/********************************************************************************************************
*                                                                                                       *
*                                    Snowflake Simple Versioning                                        *
*                                                                                                       *
*  Copyright (c) 2020, 2021 Snowflake Computing Inc. All rights reserved.                               *
*                                                                                                       *
*  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in  *
*. compliance with the License. You may obtain a copy of the License at                                 *
*                                                                                                       *
*                               http://www.apache.org/licenses/LICENSE-2.0                              *
*                                                                                                       *
*  Unless required by applicable law or agreed to in writing, software distributed under the License    *
*  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or  *
*  implied. See the License for the specific language governing permissions and limitations under the   *
*  License.                                                                                             *
*                                                                                                       *
*  Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.                                     *
*                                                                                                       *
********************************************************************************************************/

-- Recommended to keep version history in its own schema
use database UTIL_DB;
create or replace schema VERSIONING;


-- This table holds the version history
create or replace table VERSION_HISTORY
(
   ID string           default uuid_string()
  ,OBJECT_TYPE         string
  ,OBJECT_DATABASE     string
  ,OBJECT_SCHEMA       string
  ,OBJECT_NAME         string
  ,VERSION             number
  ,COMMIT_COMMENTS     string
  ,EFFECTIVE_TIMESTAMP timestamp_tz
  ,OBSOLETE_TIMESTAMP  timestamp_tz
  ,ORIGINAL_OWNER      string
  ,DESCRIPTION         string
);

-- This procedure will version the objects.
create or replace procedure VERSION(COMMIT_COMMENTS string, OBJECT_TYPE string, OBJECT_PATH string)
returns string
language javascript
as
$$

/***************************************************************************************************
* Change these constants if you use a different path to the versioning table                       *
***************************************************************************************************/

const VERSIONING_DATABASE = "UTIL_DB";
const VERSIONING_SCHEMA   = "VERSIONING";
const VERSIONING_TABLE    = "VERSION_HISTORY";

/***************************************************************************************************
* Do not modify below this line                                                                    *
***************************************************************************************************/

const DATABASE_PATH = 0;
const SCHEMA_PATH = 1;
const NAME_PATH = 2;

var currentTimestamp = new Date().toISOString();
currentTimestamp = currentTimestamp.replace(/[A-Z]/g, " ");

var objectToVersion = OBJECT_PATH.trim().split(".");

if (objectToVersion.length < 3) {
    return "Error: You must specify fully-qualified, three part object names <database>.<schema>.<object_name>";
}

var objectType;

for (var i = 0; i < 3; i++) {
    if(objectToVersion[i].indexOf('"') != -1) {
        objectToVersion[i] = objectToVersion[i].replace(/"/g, "");
    } else {
        objectToVersion[i] = objectToVersion[i].toUpperCase();
    }
}

objectType = OBJECT_TYPE.trim().toUpperCase();

var version = 1;

var sql = getCurrentVersionSQL(objectType, objectToVersion[DATABASE_PATH], objectToVersion[SCHEMA_PATH], objectToVersion[NAME_PATH],
                               VERSIONING_DATABASE, VERSIONING_SCHEMA, VERSIONING_TABLE);
var rs = getResultSet(sql);

if (rs.next()) {

    // Get the next version number
    version = rs.getColumnValue("VERSION") + 1;

    //Invalidate old version
    oldID = rs.getColumnValue("ID");
    executeNonQuery(`update "${VERSIONING_DATABASE}"."${VERSIONING_SCHEMA}"."${VERSIONING_TABLE}" set OBSOLETE_TIMESTAMP = '${currentTimestamp}' where ID = '${oldID}'`);

}

try {

    if (objectType.localeCompare("PROCEDURE") == 0 || objectType.localeCompare("FUNCTION") == 0) {
        rs = getResultSet(` select get_ddl('${objectType}', '"${objectToVersion[DATABASE_PATH]}"."${objectToVersion[SCHEMA_PATH]}".${objectToVersion[NAME_PATH]}') as DDL `);
    } else {
        rs = getResultSet(` select get_ddl('${objectType}', '"${objectToVersion[DATABASE_PATH]}"."${objectToVersion[SCHEMA_PATH]}"."${objectToVersion[NAME_PATH]}"') as DDL `);
    }
} catch(e) {
    return "Error attempting to get DDL for the object to version: " + e.message;
}

if (rs.next()) {

    var ddl = rs.getColumnValue("DDL");
    
    if (objectType.localeCompare("PROCEDURE") == 0 || objectType.localeCompare("FUNCTION") == 0) {
        ddl = getProceduralDDL(ddl);
    }

    sql = getInsertVersionSQL(objectType,
                          objectToVersion[DATABASE_PATH],
                          objectToVersion[SCHEMA_PATH],
                          objectToVersion[NAME_PATH],
                          VERSIONING_DATABASE,
                          VERSIONING_SCHEMA,
                          VERSIONING_TABLE,
                          currentTimestamp,
                          COMMIT_COMMENTS,
                          getSingleQuoted(ddl));  // This is where to get the get_ddl
    executeNonQuery(sql);
}

return `Added version ${version} of ${objectType} "${objectToVersion[DATABASE_PATH]}"."${objectToVersion[SCHEMA_PATH]}"."${objectToVersion[NAME_PATH]}".`;

/***************************************************************************************************
*  Helper functions                                                                                *
***************************************************************************************************/

function getProceduralDDL(ddlCode) {
    let lines = ddlCode.split("\n");
    let out = "";
    let startCode = new RegExp("^AS '$", "ig");
    let endCode = new RegExp("^'\;$", "ig");
    let inCode = false;
    let isChange = false;
    let s;
    for (i = 0; i < lines.length; i++){
        isChange = false;
        if(!inCode) {
            inCode = startCode.test(lines[i]);
            if(inCode) {
                isChange = true;
                out += "AS $" + "$\n";
            }
        }
        if (endCode.test(lines[i])){
            out += "$" + "$;";
            isChange = true;
            inCode = false;
        }
        if(!isChange){
            if(inCode){
                s = lines[i].replace(/''/g, "'") + "\n";
                s = s.replace(/\\\\/g, "\\");
                out += s;
            } else {
                out += lines[i] + "\n";
            }
        }
    }
    return out;
}

/***************************************************************************************************
*  SQL templates                                                                                   *
***************************************************************************************************/

function getInsertVersionSQL(objectType,
                            objectDatabase,
                            objectSchema,
                            objectName,
                            versionDatabase,
                            versionSchema,
                            versionTable,
                            currentTimestamp,
                            commitComments,
                            description) {

sql = 
`
insert  into "${versionDatabase}"."${versionSchema}"."${versionTable}" 
    (
    OBJECT_TYPE,
    OBJECT_DATABASE,
    OBJECT_SCHEMA,
    OBJECT_NAME,
    VERSION,
    EFFECTIVE_TIMESTAMP,
    COMMIT_COMMENTS,
    ORIGINAL_OWNER,
    DESCRIPTION
    )
values
    (
    '${objectType}',
    '${objectDatabase}',
    '${objectSchema}',
    '${objectName}',
     ${version},
    '${currentTimestamp}',
    '${commitComments}',
     null,
     '${description}'
    );
`;
return sql;
}

function getCurrentVersionSQL(objectType, objectDatabase, objectSchema, objectName, versionDatabase, versionSchema, versionTable){

var sql = 
`
select  * 
from    "${versionDatabase}"."${versionSchema}"."${versionTable}"
where   OBJECT_TYPE         = '${objectType}' and
        OBJECT_DATABASE     = '${objectDatabase}' and
        OBJECT_SCHEMA       = '${objectSchema}' and
        OBJECT_NAME         = '${objectName}' and
        OBSOLETE_TIMESTAMP is null
;`;

return sql;
}

/***************************************************************************************************
*  SQL functions                                                                                   *
***************************************************************************************************/

function getResultSet(sql){
    cmd1 = {sqlText: sql};
    stmt = snowflake.createStatement(cmd1);
    var rs;
    rs = stmt.execute();
    return rs;
}

function executeNonQuery(queryString) {
    var out = '';
    cmd1 = {sqlText: queryString};
    stmt = snowflake.createStatement(cmd1);
    var rs;
    rs = stmt.execute();
}

function getSingleQuoted(str) {
    return str.replace(/'/g, "''");
}
$$;
      
call version('Initial commit', 'procedure', 'UTIL_DB.VERSIONING.VERSION(string, string, string)');
