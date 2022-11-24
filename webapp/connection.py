from nzpyida import IdaDataBase
from dotenv import load_dotenv
import os
load_dotenv()


nzpy_dsn ={
        "database":"twitter",
         "port" :5480,
        "host" : os.getenv('DATABASE_HOST'),
        "securityLevel":3,
        "logLevel":0
       }

idadb = IdaDataBase(nzpy_dsn, uid="admin", pwd="password")