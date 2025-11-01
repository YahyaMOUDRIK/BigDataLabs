package edu.ensias.hadoop.mapreducelab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        int sum = 0;
        
        // Additionner toutes les valeurs
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        // Définir le résultat
        result.set(sum);
        
        // Émettre la paire (mot, somme)
        context.write(key, result);
    }
}