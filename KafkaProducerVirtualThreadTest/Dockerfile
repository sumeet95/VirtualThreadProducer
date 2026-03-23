# -------- Build stage --------
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app

# Cache dependencies first
COPY pom.xml .
RUN mvn -q -e -DskipTests dependency:go-offline

# Copy source and build
COPY src ./src
RUN mvn -q -DskipTests package

# -------- Run stage --------
FROM eclipse-temurin:17-jre
WORKDIR /app

# Copy the shaded jar
COPY --from=build /app/target/*-all.jar /app/app.jar

ENTRYPOINT ["java","-jar","/app/app.jar"]