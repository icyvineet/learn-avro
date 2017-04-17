package com.vineet.serde;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import com.vineet.model.Column;
import com.vineet.model.HBaseRowData;

public class AvroDeserializer {
	public static void main(String args[]) throws Exception {
		String schemaLocation = "C:\\Users\\icyvi\\workspace\\learn-avro\\src\\main\\resources\\avroSchema.avsc";
		String inputFileLocation = "C:\\Users\\icyvi\\workspace\\learn-avro\\target\\avroSerialized.avro";
		String outputFileLocation = "C:\\Users\\icyvi\\workspace\\learn-avro\\target\\avroDeSerialized.json";

		if (args.length == 3) {
			schemaLocation = args[0];
			inputFileLocation = args[1];
			outputFileLocation = args[2];
		}

		
		Schema schema = new Schema.Parser().parse(new File(schemaLocation));
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(inputFileLocation), datumReader);
		GenericRecord record = null;
		
		List<HBaseRowData> hbrdList = new ArrayList<HBaseRowData>();
		while (dataFileReader.hasNext()){
			record = dataFileReader.next(record);
			
			HBaseRowData hbrd = new HBaseRowData();
			
			
			hbrd.setRowKey(record.get("rowKey").toString());
			hbrd.setColumnFamily(record.get("columnFamily").toString());
			
			GenericData.Array<GenericRecord> columnArrayData = (GenericData.Array<GenericRecord>) record.get("columns");
			
			List<Column> columns = new ArrayList<Column>();
			for(GenericRecord columnGenericRecord : columnArrayData){
				Column column = new Column();
				column.setKey(columnGenericRecord.get("key").toString());
				column.setValue(columnGenericRecord.get("value").toString());
				columns.add(column);
			}
			hbrd.setColumns(columns);
			hbrdList.add(hbrd);
		}

		hbrdList.forEach(System.out::println);
		System.out.println("Completed Printing of all the records...!!");
	}
}
