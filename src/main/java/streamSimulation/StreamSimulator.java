package streamSimulation;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.javatuples.Pair;

import java.io.*;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class StreamSimulator implements Runnable {

    private Thread t;
    private SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private int maxDelayMsecs=0;
    private int servingSpeed=1000;
    private BufferedReader reader;
    private InputStream fileStream;
    private Producer<String,String> producer=null;
    private String startDate="2015-05-01 00:00:00";
    private String file="/media/xiaokeai/Study/data/0501ordered/100.txt";
    private String name;


    StreamSimulator()
    {

    }

    StreamSimulator(String name,int maxDelayMsecs,int servingSpeed,String startDate,String file)
    {
        this.maxDelayMsecs=maxDelayMsecs;
        this.servingSpeed=servingSpeed;
        this.startDate=startDate;
        this.file=file;
        this.name=name;
    }
    @Override
    public void run(){

        try {
            taxiDataSource();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    public void start()
    {
        if (t==null)
        {
            t=new Thread(this,name);
            t.start();
        }
    }
    public static void main(String[] args) throws Exception
    {
        int maxDelayMsecs=0;
        int servingSpeed=100;
        String startDate="2015-05-01 00:00:00";
        StreamSimulator simulator1=new StreamSimulator("100",maxDelayMsecs,servingSpeed,startDate
                ,"/media/xiaokeai/Study/data/0501ordered/100.txt");
        simulator1.start();

        StreamSimulator simulator2=new StreamSimulator("101",maxDelayMsecs,servingSpeed,startDate
                ,"/media/xiaokeai/Study/data/0501ordered/101.txt");
        simulator2.start();

        StreamSimulator simulator3=new StreamSimulator("103",maxDelayMsecs,servingSpeed,startDate
                ,"/media/xiaokeai/Study/data/0501ordered/103.txt");
        simulator3.start();

        StreamSimulator simulator4=new StreamSimulator("104",maxDelayMsecs,servingSpeed,startDate
                ,"/media/xiaokeai/Study/data/0501ordered/104.txt");
        simulator4.start();

        StreamSimulator simulator5=new StreamSimulator("105",maxDelayMsecs,servingSpeed,startDate
                ,"/media/xiaokeai/Study/data/0501ordered/105.txt");
        simulator5.start();

    }

    private void taxiDataSource() throws Exception
    {
        fileStream=new FileInputStream(this.file);
        reader=new BufferedReader(new InputStreamReader(fileStream,"UTF-8"));
        generateUnorderedStream(this.startDate);

        this.reader.close();
        this.reader=null;
        this.fileStream.close();
        this.fileStream=null;
        producer.close();
        producer=null;

    }


    private  void generateUnorderedStream(String date)
    {
        initKafkaProducer();
        long servingStartTime= Calendar.getInstance().getTimeInMillis();
        long dataStartTime=0L;
        try {
             dataStartTime=dateFormat.parse(date).getTime();
        }
        catch (ParseException e)
        {
            e.printStackTrace();
        }
        Random rand=new Random(999);
        PriorityQueue<Pair<Long,String>> emitSchedule=new PriorityQueue<>(32,
                new Comparator<Pair<Long, String>>() {
                    @Override
                    public int compare(Pair<Long, String> o1, Pair<Long, String> o2) {
                        return o1.getValue0().compareTo(o2.getValue0());
                    }
                });

        //read the first ride
        String line;
        try{
            if(reader.ready()&&(line=reader.readLine())!=null)
            {
                long rideEventTime=dateFormat.parse(line.split(",")[1]).getTime();
                long delayedEventTime=rideEventTime+getNormalDelayMsecs(rand);
                emitSchedule.add(new Pair<Long,String>(delayedEventTime,line));

            }
            else{
                return;
            }

            if (reader.ready() && (line = reader.readLine()) != null) {
            }
            while (emitSchedule.size()>0 || reader.ready())
            {
                long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().getValue0() : -1;
                long rideEventTime=dateFormat.parse(line.split(",")[1]).getTime();
                while (line!=null && (emitSchedule.isEmpty()||rideEventTime<curNextDelayedEventTime+maxDelayMsecs))
                {
                    long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
                    emitSchedule.add(new Pair<Long, String>(delayedEventTime, line));

                    if(reader.ready()&&(line=reader.readLine())!=null)
                    {
                        rideEventTime=dateFormat.parse(line.split(",")[1]).getTime();
                    }
                    else {
                        line=null;
                        rideEventTime=-1;
                    }
                }

                Pair<Long,String> head=emitSchedule.poll();
                long delayedEventTime = head.getValue0();
                String message_line=head.getValue1();

                long now=Calendar.getInstance().getTimeInMillis();
                long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
                long waitTime = servingTime - now;

                Thread.sleep( (waitTime > 0) ? waitTime : 0);

                sendToKafka(message_line.split(",")[0],message_line);


            }

        }


        catch (Exception e)
        {
            e.printStackTrace();
        }


    }

    private long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while(delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    private long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    private void initKafkaProducer()
    {
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer"
                ,"org.apache.kafka.common.serialization.StringSerializer");
        producer=new KafkaProducer(props);
    }
    private void sendToKafka(String id,String message)
    {
        ProducerRecord<String,String> record=new ProducerRecord<>("taxi1",id,message);
        producer.send(record);
        System.out.println(id+"   "+message);
    }


}
