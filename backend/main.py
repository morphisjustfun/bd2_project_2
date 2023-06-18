import sys

from src.spimi_inverted_index import SPIMIInvertedIndex
from multiprocessing import cpu_count
import time
from sqlalchemy import create_engine

if __name__ == "__main__":
    query = sys.argv[1]
    k = int(sys.argv[2])
    test = SPIMIInvertedIndex(data_file_name="data/data.json", output_dir="dist/", block_size=50000)
    # test.clean()
    # test.build()
    # test.merge_blocks()
    # test.preprocess_dist()
    # create engine for bd2_project2 database with user postgres and no password
    engine = create_engine('postgresql://postgres@localhost:5432/bd2_project2')
    # create connection to database
    conn = engine.connect()

    # create
    # table if not exists
    # main
    # (
    #     id        varchar primary key,
    # submitter varchar,
    # title     varchar,
    # doi       varchar,
    # abstract  varchar
    # );
    # execute following query

    # SELECT *, similarity(abstract, {query})
    # AS
    # score
    # FROM
    # main
    # ORDER
    # BY
    # score
    # DESC
    # LIMIT
    # {K};

    # and measure time of execution
    startPostgreSQL = time.perf_counter()
    queryResult = conn.execute(
        f"SELECT *, similarity(abstract, '{query}') AS score FROM main ORDER BY score DESC LIMIT {k};")
    endPostgreSQL = time.perf_counter()
    timePostgreSQLMs = endPostgreSQL - startPostgreSQL

    queryResultPostgresql = [{'doc_id': row[0], 'score': row[4], 'submitter': row[1], 'title': row[2], 'doi': row[3]}
                             for row in
                             queryResult]

    startPython = time.perf_counter()
    queryResult = test.query(query, k)
    endPython = time.perf_counter()
    # expand each value of queryResult with values of 'submitter', 'title', 'doi' using the database
    for i in range(len(queryResult)):
        doc_id = queryResult[i]['doc_id']
        connResult = conn.execute(f"SELECT * FROM main WHERE id = '{doc_id}'")
        connResult = connResult.fetchone()
        queryResult[i]['submitter'] = connResult[1]
        queryResult[i]['title'] = connResult[2]
        queryResult[i]['doi'] = connResult[3]


    timePythonMs = endPython - startPython
    resultPython = {
        'result': queryResult,
        'time': timePythonMs,
    }

    resultPostgreSQL = {
        'result': queryResultPostgresql,
        'time': timePostgreSQLMs
    }

    finalResult = {
        'python': resultPython,
        'postgreSQL': resultPostgreSQL
    }

    # close connection to database
    print(finalResult)
    conn.close()
