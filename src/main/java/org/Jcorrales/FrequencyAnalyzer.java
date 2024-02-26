package org.Jcorrales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.Map;

public class FrequencyAnalyzer {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable uno = new IntWritable(1);
        private Text parPalabras = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer line = new StringTokenizer(value.toString());
            int cantTokens = line.countTokens();
            String[] palabras = new String[cantTokens];
            String[] pareja = new String[2];
            int cantPalabras = 0; //iterador

            while (line.hasMoreTokens()) {
                palabras[cantPalabras] = line.nextToken();
                cantPalabras++;
            }

            if (cantPalabras > 2) {
                for (int actual = 0; actual < cantPalabras - 1; actual++) {
                    for (int siguiente = actual + 1; siguiente < cantPalabras; siguiente++) {
                        pareja[0] = palabras[actual];
                        pareja[1] = palabras[siguiente];

                        // Si es la misma palabra que no la escriba
                        if (!pareja[0].equals(pareja[1])) {
                            Arrays.sort(pareja); // ordenar el par de palabras para que sean iguales (hola como) == (como hola)

                            parPalabras.set(pareja[0] + " " + pareja[1]);
                            context.write(parPalabras, uno);
                        }
                    }
                }
            }

            // Si solo hay 2 palabras en la linea y no son las mismas, que la registre
            if (cantPalabras == 2 && !palabras[0].equals(palabras[1])) {
                Arrays.sort(palabras);
                // Formato en el que se guarda 'palabra1 palabra2 1'
                parPalabras.set(palabras[0] + " " + palabras[1]);
                context.write(parPalabras, uno);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<Text, Integer> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int suma = 0;
            for (IntWritable value : values) {
                suma += value.get();
            }
            countMap.put(new Text(key), suma);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            countMap.entrySet().stream()
                    .sorted((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()))
                    .limit(100)  // Que al final me de 300 registros
                    .forEach(entry -> {
                        try {
                            context.write(entry.getKey(), new IntWritable(entry.getValue()));
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Frequency Analyzer");
        job.setJarByClass(FrequencyAnalyzer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
