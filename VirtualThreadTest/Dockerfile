# -------- Build stage --------
FROM maven:3.9.12-eclipse-temurin-25 AS build
WORKDIR /app

# Cache dependencies first
COPY pom.xml .
RUN mvn -q -e -DskipTests dependency:go-offline

# Copy source and build
COPY src ./src
RUN mvn -q -DskipTests package

# -------- Run stage --------
FROM eclipse-temurin:25-jre
WORKDIR /app


# Copy shaded jar produced by maven-shade-plugin (classifier "all")
COPY --from=build /app/target/*-all.jar /app/app.jar

# (Optional) safer container defaults
ENV JAVA_OPTS=""

# Run the jar (manifest already contains Main-Class)
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]