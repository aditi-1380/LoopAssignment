# LoopAssignment

1. Overview
This project implements a data ingestion system with:
- **Asynchronous Batch Processing**  
- **Rate-limited** to 1 batch every 5 seconds  
- **Priority Queueing** (HIGH > MEDIUM > LOW)  
- **Batch Size Limit** of 3 IDs per batch  
- Real-time status tracking via `/status/:ingestion_id`  

2.  How It Works
   **POST /ingest**  
   Accepts a list of IDs and priority.  
   IDs are broken into batches (max 3 IDs each).  
   Each batch is queued and processed asynchronously based on:
   - Priority (HIGH > MEDIUM > LOW)
   - Timestamp (FIFO)
   **GET /status/:ingestion_id**  
   Returns status of ingestion (`yet_to_start`, `triggered`, or `completed`)  
   Includes batch details and their individual statuses.

3. Install Dependencies and Start it
   1. npm install
   2. npm start
