/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.uom.cse.mapreduceassignment;

/**
 *
 * @author Ashan Peiris
 */
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MedicineCostMapper extends Mapper<Object, Text, Text, FloatWritable> {
	private TreeMap<Float, String> treeMap;

	@Override
	public void setup(Mapper.Context context) throws IOException, InterruptedException
	{
		treeMap = new TreeMap<>(); // Use TreeMap to get sorted values
	}

	@Override
	public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException
	{
		String[] tokens = value.toString().split("\t");
		String medicine = tokens[0];
		Float price = Float.parseFloat(tokens[1]);
		treeMap.put(price, medicine);
		if (treeMap.size() > 100) // To make sure, we always have top 100 medical costs
		{
			treeMap.remove(treeMap.firstKey());
		}
	}

	@Override
	public void cleanup(Mapper.Context context) throws IOException, InterruptedException
	{
		for (Map.Entry<Float, String> entry : treeMap.entrySet())
		{
			Float price = entry.getKey();
			String medicine = entry.getValue();
			context.write(new Text(medicine), new FloatWritable(price));
		}
	}
}
