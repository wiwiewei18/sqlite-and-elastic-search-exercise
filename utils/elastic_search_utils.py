import uuid


def df_to_elasticsearch(df, index_name):
    for i, row in df.iterrows():
        yield {
            "_index": index_name,
            "_id": str(uuid.uuid4()),
            "_source": row.to_dict(),
        }
