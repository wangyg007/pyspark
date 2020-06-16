
from datahub import DataHub
from datahub.models import CursorType,RecordSchema,FieldType


access_id="***"
access_key="***"
endpoint="***"
dhb=DataHub(access_id,access_key,endpoint)

project_name="zj_binlog"
topic_name="zj_binlog"

if __name__=="__main__":
    # shard_res = dhb.list_shard(project_name, topic_name)
    # print(shard_res.shards)

    cursor_latest_res = dhb.get_cursor(project_name,topic_name,"0",CursorType.LATEST)
    cursor = cursor_latest_res.cursor

    record_schema = RecordSchema.from_lists(
        ['srcjdbcurl', 'eventtype', 'tablename','executetime','json','yyyymmdd'],
        [FieldType.STRING,FieldType.STRING,FieldType.STRING,FieldType.STRING,FieldType.STRING,FieldType.STRING]
    )
    records = dhb.get_tuple_records(project_name, topic_name, "0", record_schema, cursor, 10)

    print(records.records)