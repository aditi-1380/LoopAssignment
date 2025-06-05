const express = require('express');
const { v4: uuidv4 } = require('uuid');
const app = express();
app.use(express.json());

const PORT = 5000;

const PRIORITY_MAP = { HIGH: 1, MEDIUM: 2, LOW: 3 };

let globalBatchQueue = [];
let ingestionStore = {};


function createBatches(ids, batchSize = 3) {
  const batches = [];
  for (let i = 0; i < ids.length; i += batchSize) {
    batches.push(ids.slice(i, i + batchSize));
  }
  return batches;
}

app.post('/ingest', (req, res) => {
  const { ids, priority } = req.body;
  if (!ids || !priority) {
    return res.status(400).json({ error: 'Missing ids' });
  }


  if (!PRIORITY_MAP.hasOwnProperty(priority)) {
    return res.status(400).json({ error: 'Invalid value' });
  }

  const ingestion_id = uuidv4();

  const batches = createBatches(ids);
  ingestionStore[ingestion_id] = {
    ingestion_id,
    priority,
    status: 'yet_to_start',
    batches: []
  };

  batches.forEach(batchIds => {
    const batch_id = uuidv4();
    const batchObj = {
      batch_id,
      ids: batchIds,
      ingestion_id,
      status: 'yet_to_start',
    };
    ingestionStore[ingestion_id].batches.push(batchObj);

    globalBatchQueue.push({ ...batchObj });
  });

  res.json({ ingestion_id });
});

app.get('/status/:ingestion_id', (req, res) => {
  const { ingestion_id } = req.params;
  const record = ingestionStore[ingestion_id];
  if (!record) {
    return res.status(404).json({ error: 'Ingestion ID not found' });
  }


  const statuses = record.batches.map(b => b.status);
  if (statuses.every(s => s === 'completed')) record.status = 'completed';
  else if (statuses.some(s => s === 'triggered')) record.status = 'triggered';
  else record.status = 'yet_to_start';

  res.json({ ingestion_id, status: record.status, batches: record.batches });
});

setInterval(() => {
  if (globalBatchQueue.length === 0) return;

  globalBatchQueue.sort((a, b) => {
    const p1 = PRIORITY_MAP[ingestionStore[a.ingestion_id].priority];
    const p2 = PRIORITY_MAP[ingestionStore[b.ingestion_id].priority];
    if (p1 !== p2) return p1 - p2;
  });

  const batchToProcess = globalBatchQueue.shift();

  const ingestion = ingestionStore[batchToProcess.ingestion_id];
  const batchInIngestion = ingestion.batches.find(b => b.batch_id === batchToProcess.batch_id);
  if (!batchInIngestion) return;

  batchInIngestion.status = 'triggered';

  setTimeout(() => {
    batchInIngestion.status = 'completed';

    console.log(`Processed batch ${batchToProcess.batch_id} for ingestion ${batchToProcess.ingestion_id}`);

    if (ingestion.batches.every(b => b.status === 'completed')) {
      ingestion.status = 'completed';
    }
  }, 5000);

}, 5000);

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:5000`);
});