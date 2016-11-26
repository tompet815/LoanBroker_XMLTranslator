package translator;

import com.rabbitmq.client.AMQP.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import connector.RabbitMQConnector;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import models.Data;
import models.XMLData;
import utilities.MessageUtility;

public class XMLTranslator {

    private final RabbitMQConnector connector = new RabbitMQConnector();

    private Channel channel;
    private String queueName;
    private final String EXCHANGENAME = "whatTranslator";
    private final String ROUTING_KEY="xml";
    private final String BANKEXCHANGENAME = "cphbusiness.bankXML";
    private final MessageUtility util = new MessageUtility();
    private final String REPLYTOQUENAME = "whatNormalizerQueue";

    public void init() throws IOException {
        channel = connector.getChannel();
        channel.exchangeDeclare(EXCHANGENAME, "direct");
        queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGENAME, ROUTING_KEY);
        receive();
    }

    private boolean receive() throws IOException {

        System.out.println(" [*] Waiting for messages.");

        final Consumer consumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                try {
                    System.out.println(" [x] Received ");
                    send(properties, body);
                }
                finally {
                    System.out.println(" [x] Done");
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        channel.basicConsume(queueName, false, consumer);
        return true;
    }

    private BasicProperties propBuilder(String corrId, Map<String, Object> headers) {
        BasicProperties.Builder builder = new BasicProperties.Builder();
        builder.replyTo(REPLYTOQUENAME);
        builder.headers(headers);
        builder.correlationId(corrId);
        BasicProperties prop = builder.build();
        return prop;
    }

    private Data unmarchal(String bodyString) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Data.class);
        Unmarshaller unmarchaller = jc.createUnmarshaller();
        StringReader reader = new StringReader(bodyString);
        return (Data) unmarchaller.unmarshal(reader);
    }

    private String removeBom(String xmlString) {
        String res = xmlString.trim();
        return res.substring(res.indexOf("<?xml"));
    }

    public boolean send(BasicProperties prop, byte[] body) throws IOException {

        try {
            JAXBContext jc = JAXBContext.newInstance(XMLData.class);
            String bodyString = removeBom(new String(body));
            Data data = unmarchal(bodyString);
             String ssn=data.getSsn();
            String ssnWithoutBind=ssn.replace("-","");
            data.setSsn(ssnWithoutBind);            
            System.out.println("sending SSN :"+ssnWithoutBind);
            XMLData xmlData = util.convertToXMLData(data);
            Marshaller marshaller = jc.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            JAXBElement<XMLData> je2 = new JAXBElement(new QName("LoanRequest"), XMLData.class, xmlData);
            StringWriter sw = new StringWriter();
            marshaller.marshal(je2, sw);
            String xmlString = sw.toString();
            System.out.println("xml" + xmlString);
            String corrId = prop.getCorrelationId();
            BasicProperties newProp = propBuilder(corrId, prop.getHeaders());
            channel.basicPublish(BANKEXCHANGENAME, "", newProp, xmlString.getBytes());
            return true;
        }
        catch (JAXBException ex) {
            Logger.getLogger(XMLTranslator.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
}
