-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS myapp
WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};

-- Create a table with a vector column
CREATE TABLE IF NOT EXISTS myapp.comments (
  id uuid PRIMARY KEY,
  comment text,
  comment_vector vector<float, 5>
);

-- Insert sample data
INSERT INTO myapp.comments (id, comment, comment_vector)
VALUES (uuid(), 'I like vector search!', [0.12, 0.34, 0.56, 0.78, 0.91]);

INSERT INTO myapp.comments (id, comment, comment_vector)
VALUES (uuid(), 'ScyllaDB is great!', [0.11, 0.35, 0.55, 0.77, 0.92]);

-- Create a vector index for ANN queries
CREATE CUSTOM INDEX IF NOT EXISTS comment_ann_index
ON myapp.comments(comment_vector) USING 'vector_index';

-- ** WAIT FEW SECONDS FOR THE INDEX TO BE CREATED **

-- Run a similarity search
SELECT comment FROM myapp.comments
ORDER BY comment_vector ANN OF [0.12, 0.34, 0.56, 0.78, 0.91] LIMIT 1;
