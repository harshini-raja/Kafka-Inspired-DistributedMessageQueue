# Use the official OpenJDK 17 image as the base
FROM openjdk:17-jdk-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the built JAR file into the container (replace with your actual JAR name)
COPY target/DistributedMessageQueue-1.0-SNAPSHOT.jar /app/broker-application.jar

# Set environment variables (optional) for configuration
ENV JAVA_OPTS=""

# Define the command to run the broker application, including any Java options
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/broker-application.jar"]

CMD ["java", "-cp", "distqueue-application.jar", "com.distqueue.MainClass"]

