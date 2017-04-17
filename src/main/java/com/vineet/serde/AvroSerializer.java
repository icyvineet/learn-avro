package com.vineet.serde;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vineet.model.Column;
import com.vineet.model.HBaseRowData;

public class AvroSerializer {

	public static void main(String[] args) throws IOException {

		String schemaLocation = "C:\\Users\\icyvi\\workspace\\learn-avro\\src\\main\\resources\\avroSchema.avsc";
		String inputFileLocation = "C:\\Users\\icyvi\\workspace\\learn-avro\\src\\main\\resources\\jsonRecord.json";
		String outputFileLocation = "C:\\Users\\icyvi\\workspace\\learn-avro\\target\\avroSerialized.avro";

		if (args.length == 3) {
			schemaLocation = args[0];
			inputFileLocation = args[1];
			outputFileLocation = args[2];
		}

		
		Schema schema = new Schema.Parser().parse(new File(schemaLocation));
		
		List<String> jsonRowList = Files.readAllLines(Paths.get(inputFileLocation));
		
		
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.setCodec(CodecFactory.snappyCodec());
		dataFileWriter.create(schema, new File(outputFileLocation));
		
		ObjectMapper om = new ObjectMapper();
		
		for(String jsonRow : jsonRowList){
			HBaseRowData hbaseRowData = om.readValue(jsonRow, HBaseRowData.class);
					
			GenericRecord record = new GenericData.Record(schema);
			record.put("rowKey", hbaseRowData.getRowKey());
			record.put("columnFamily", hbaseRowData.getColumnFamily());
			
			Schema columnsSchema = schema.getField("columns").schema();
			Schema columnSchema = columnsSchema.getElementType();
			
			GenericData.Record columnGenericRecord = null;//new GenericData.Record(columnSchema);
			
			GenericData.Array<GenericData.Record> columnArrayData = new GenericData.Array<GenericData.Record>(hbaseRowData.getColumns().size(),columnsSchema);
			
			for(Column column : hbaseRowData.getColumns()){
				columnGenericRecord = new GenericData.Record(columnSchema);
				columnGenericRecord.put("key", column.getKey());
				columnGenericRecord.put("value", column.getValue());
				
				System.out.println("columnGenericRecord:--->> " + columnGenericRecord);
				columnArrayData.add(columnGenericRecord);

			}
			record.put("columns", columnArrayData);
			dataFileWriter.append(record);
			
		}
		dataFileWriter.close();
		System.out.println("Completed Successfully....!!!");
	}

}
