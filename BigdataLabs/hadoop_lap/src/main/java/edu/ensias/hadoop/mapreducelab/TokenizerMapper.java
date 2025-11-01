package edu.ensias.hadoop.mapreducelab;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    
    // Variables réutilisables (pour optimiser)
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        // Afficher la clé pour débogage
        System.out.println("Processing line at offset: " + key.toString());
        
        // Découper la ligne en mots
        StringTokenizer itr = new StringTokenizer(value.toString());
        
        // Pour chaque mot
        while (itr.hasMoreTokens()) {
            // Récupérer le mot
            word.set(itr.nextToken());
            
            // Émettre la paire (mot, 1)
            context.write(word, one);
        }
    }
}
