# ForEachBatch sink: write_foreachbatch_sink

@dp.foreach_batch_sink(name="my_batch_sink")
def my_batch_sink(df, batch_id):
    """ForEachBatch sink: write_foreachbatch_sink"""
    df.write.format('delta').mode('append').saveAsTable('target_table')
    return

@dp.append_flow(target="my_batch_sink", name="f_my_batch_sink_1", comment="ForEachBatch sink: write_foreachbatch_sink")
def f_my_batch_sink_1():
    df = spark.readStream.table("v_data")
    
    
    return df
