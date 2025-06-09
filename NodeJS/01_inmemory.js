const utils         = require("./00_utils");
const {MongoClient} = require("mongodb");

async function inmemory() {   
    let client        = new MongoClient(process.env.MONGO_URI);
    let db            = client.db();

    let colls=[];
    let collName = "";
    let numColls = 0;

    try {
        let oracle_api = !(await utils.isNativeMongoDB(db));
        if (!oracle_api) {
            console.log("This db looks like a native MongoDB system. The demo has been developed to work on Oracle API for MongoDB only. Exiting.");
            process.exit(0);
        }  
        console.log("You are connected to an Oracle API for MongoDB instance.");
        // 1. no inmemory no parallel
        console.log("Test #1 : no inmemory, noparallel");
        result = db.aggregate([{ $sql: "alter table jsonflights_col_large noparallel no inmemory"}]);
        for await (res of result);
        cached = true;
        while (cached) {
            result = db.aggregate([{ $sql: "select count(*) tloaded from v$im_segments where owner = user and segment_name = 'JSONFLIGHTS_COL_LARGE' and populate_status = 'COMPLETED'"}]);
            for await (res of result) {
                if  (res.TLOADED == 0)
                    cached = false;
            }
            process.stdout.write('#');
            utils.sleep(1000);                
        } 
        console.log(" ");       
        console.log("Collection not in InMemory cache.");
        // 1. a. counting documents using MongoDB native commands.
        console.log("1. a. MongoDB native command");
        startTimeMDB = Date.now();
        numOfDocsMDB = await db.collection("JSONFLIGHTS_COL_LARGE").countDocuments();
        endTimeMDB   = Date.now();
        execTimeMDB  = endTimeMDB - startTimeMDB;
        console.log("No inmemory, No parallel, MongoDB native command execution test. Execution time (ms): "+execTimeMDB);
        
        // 1. b. counting documents using SQL
        console.log("1. b. SQL command");
        startTimeSQL = Date.now();
        result = db.aggregate([{ $sql: "select count(*) from jsonflights_col_large"}],{"hint" : {"$service" : "TPURGENT"}});
        for await (res of result);
        endTimeSQL   = Date.now();
        execTimeSQL  = endTimeSQL - startTimeSQL;
        console.log("No inmemory, No parallel, SQL execution test. Execution time (ms): "+execTimeSQL);
        console.log("SQL execution plan : ");
        await utils.displaySQLExecutionPlan(db,"select count(*) from jsonflights_col_large");
        
        // 2. no in memory parallel 16
        console.log("Test #2 : no inmemory, parallel 16");
        result = db.aggregate([{ $sql: "alter table jsonflights_col_large parallel 16"}]);
        for await (res of result);

        // 2. a. counting documents using MongoDB native commands. 
        console.log("2. a. MongoDB native command");
        startTimeMDB = Date.now();
        numOfDocsMDB = await db.collection("JSONFLIGHTS_COL_LARGE").countDocuments();
        endTimeMDB   = Date.now();
        execTimeMDB  = endTimeMDB - startTimeMDB;
        console.log("No inmemory, Parallel execution test. Execution time (ms): "+execTimeMDB);
        console.log(await db.collection("JSONFLIGHTS_COL_LARGE").countDocuments());
        // 2. b. counting documents using SQL
        console.log("2. b. SQL command");
        startTimeSQL = Date.now();
        result = db.aggregate([{ $sql: "select /*+ MONITOR */ count(*) from jsonflights_col_large"}],{"hint" : {"$service" : "TPURGENT"}});
        for await (res of result);
        endTimeSQL   = Date.now();
        execTimeSQL  = endTimeSQL - startTimeSQL;
        console.log("No inmemory, parallel 16, SQL execution test. Execution time (ms): "+execTimeSQL);
        console.log("SQL execution plan : ");
        await utils.displaySQLExecutionPlan(db,"select count(*) from jsonflights_col_large");
        
        // 3. in memory parallel 16
        console.log("Test #3 : inmemory, parallel 16");
        console.log("Loading test collection into memory");
        result = db.aggregate([{ $sql: "alter table jsonflights_col_large inmemory priority critical duplicate"}]);
        for await (res of result);
        loading = true;
        while (loading) {
            result = db.aggregate([{ $sql: "select count(*) tloaded from v$im_segments where owner = user and segment_name = 'JSONFLIGHTS_COL_LARGE' and populate_status = 'COMPLETED'"}]);
            for await (res of result) {
                if  (res.TLOADED > 0)
                    loading = false;
            }
            process.stdout.write('#');
            utils.sleep(1000);                
        }
        console.log(" ");
        console.log("Test collection loaded");
        
         // 3. a. counting documents using MongoDB native commands. 
        console.log("3. a. MongoDB native command");
        startTimeMDB = Date.now();
        numOfDocsMDB = await db.collection("JSONFLIGHTS_COL_LARGE").countDocuments();
        endTimeMDB   = Date.now();
        execTimeMDB  = endTimeMDB - startTimeMDB;
        console.log("InMemory, Parallel execution test. Execution time (ms): "+execTimeMDB);

        // 3. b. counting documents using SQL
        console.log("3. b. SQL command");
        startTimeSQL = Date.now();
        result = db.aggregate([{ $sql: "select count(*) from jsonflights_col_large"}],{"hint" : {"$service" : "TPURGENT"}});
        for await (res of result);
        endTimeSQL   = Date.now();
        execTimeSQL  = endTimeSQL - startTimeSQL;
        console.log("InMemory, parallel 16, SQL execution test. Execution time (ms): "+execTimeSQL);
        console.log("SQL execution plan : ");
        await utils.displaySQLExecutionPlan(db,"select count(*) from jsonflights_col_large");

        // 4. in memory noparallel
        console.log("Test #4 : inmemory, noparallel");
        result = db.aggregate([{ $sql: "alter table jsonflights_col_large noparallel"}]);
        for await (res of result);

         // 4. a. counting documents using MongoDB native commands. 
        console.log("4. a. MongoDB native command");
        startTimeMDB = Date.now();
        numOfDocsMDB = await db.collection("JSONFLIGHTS_COL_LARGE").countDocuments();
        endTimeMDB   = Date.now();
        execTimeMDB  = endTimeMDB - startTimeMDB;
        console.log("InMemory, noparallel execution test. Execution time (ms): "+execTimeMDB);

        // 4. b. counting documents using SQL
        console.log("4. b. SQL command");
        startTimeSQL = Date.now();
        result = db.aggregate([{ $sql: "select count(*) from jsonflights_col_large"}],{"hint" : {"$service" : "TPURGENT"}});
        for await (res of result);
        endTimeSQL   = Date.now();
        execTimeSQL  = endTimeSQL - startTimeSQL;
        console.log("InMemory, noparallel, SQL execution test. Execution time (ms): "+execTimeSQL); 
        console.log("SQL execution plan : ");
        await utils.displaySQLExecutionPlan(db,"select count(*) from jsonflights_col_large");
    }
    catch (e) {
        console.error(e);
    }  
    finally {
        await client.close();
        console.log("Disconnected from database.");
    }
}

inmemory().catch(console.error);