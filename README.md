This is the Example For AWS MemoryDB.

Tools used:
    Visual Studio - IDE
    Redis Insight - Tool for DB
    AWS - cloud
    Postman - API testing

Steps to Follow
1. Check the primaryEndpoint for the cluster created in AWS and add it in Startup.cs
   eg. var primaryEndpoint = "clustercfg.orm-test.adb7tr.memorydb.us-east-1.amazonaws.com";
2. For DB UI install redis insight and use the cluster endpoint and port.
3. Run the program and Use different Endpoints to test.
