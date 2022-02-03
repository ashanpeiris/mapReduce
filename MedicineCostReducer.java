/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.uom.cse.mapreduceassignment;

/**
 *
 * @author Ashan Peiris
 */
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedicineCostReducer extends Reducer<Text, FloatWritable, FloatWritable, Text> {
	private TreeMap<Float, String> treeMap;

	@Override
	public void setup(Reducer.Context context) throws IOException, InterruptedException
	{
		treeMap = new TreeMap<>(); // Use TreeMap to get sorted values
	}

	public void reduce(Text key, Iterable<FloatWritable> values, Reducer.Context context) throws IOException, InterruptedException
	{
		String medicine = key.toString();
		Float price = 0f;
		for (FloatWritable val : values)
		{
                    price = val.get();
		}
		treeMap.put(price, medicine);
		if (treeMap.size() > 100) // To make sure, we always have top 100 medical costs
		{
			treeMap.remove(treeMap.firstKey());
		}
	}

	@Override
	public void cleanup(Reducer.Context context) throws IOException, InterruptedException
	{
		for (Map.Entry<Float, String> entry : treeMap.entrySet())
		{
			Float price = entry.getKey();
			String medicine = entry.getValue();
                        context.write(new FloatWritable(price), new Text(medicine));
		}
	}
}

