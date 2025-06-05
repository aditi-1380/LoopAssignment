const { v4: uuidv4 } = require("uuid");
const { store } = require("./storage");

const priorityQueue = {
  HIGH: [],
  MEDIUM: [],
  LOW: [],
};

function enqueueBatches(ids, priority, ingestionId) {
  const batches = [];
  for (let i = 0; i < ids.length; i += 3) {
    const batchIds = ids.slice(i, i + 3);
    const batch = {
      batch_id: uuidv4(),
      ids: batchIds,
      status: "yet_to_start",

    };
    priorityQueue[priority].push({ ingestionId, batch });
    batches.push(batch);
  }
  store[ingestionId] = {
    status: "yet_to_start",
    priority, 
    batches,
  };
}

function getStatus(ingestionId) {
  return store[ingestionId];
}

function updateMainStatus(ingestionId) {
  if (!store[ingestionId]) return;

  const statuses = store[ingestionId].batches.map((b) => b.status);

  if (statuses.every((s) => s === "yet_to_start")) {
    store[ingestionId].status = "yet_to_start";
  } else if (statuses.every((s) => s === "completed")) {
    store[ingestionId].status = "completed";
  } else {
    store[ingestionId].status = "triggered";
  }
}

async function processBatch(batch) {
  batch.status = "triggered";

  await new Promise((resolve) => setTimeout(resolve, 5000));

  batch.status = "completed";
}

function startWorker() {
  setInterval(async () => {
    for (const level of ["HIGH", "MEDIUM", "LOW"]) {
      if (priorityQueue[level].length > 0) {
        const { ingestionId, batch } = priorityQueue[level].shift();

        await processBatch(batch);
        updateMainStatus(ingestionId);

        break; 
      }
    }
  }, 5000);
}

module.exports = { enqueueBatches, getStatus, startWorker };