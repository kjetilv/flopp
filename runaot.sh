export CP="/Users/kjetilvalstadsve/Development/git/flopp/flopp-lc/build/libs/flopp-lc-0.1.0-SNAPSHOT.jar:/Users/kjetilvalstadsve/Development/git/flopp/flopp-kernel/build/libs/flopp-kernel-0.1.0-SNAPSHOT.jar:/Users/kjetilvalstadsve/.m2/repository/ch/qos/logback/logback-classic/1.5.6/logback-classic-1.5.6.jar:/Users/kjetilvalstadsve/.m2/repository/ch/qos/logback/logback-core/1.5.6/logback-core-1.5.6.jar:/Users/kjetilvalstadsve/.m2/repository/org/slf4j/slf4j-api/2.0.13/slf4j-api-2.0.13.jar com.github.kjetilv.flopp.ca.CalculateAverage_kjetilvlong /Users/kjetilvalstadsve/Development/data/measurements.txt /Users/kjetilvalstadsve/Development/data/fasit.txt /Users/kjetilvalstadsve/Development/data/out.txt"

/Users/kjetilvalstadsve/.sdkman/candidates/java/24-graalce/bin/java \
   -Xmx1G -Xms1G \
   -XX:+UnlockExperimentalVMOptions -XX:+UnlockDiagnosticVMOptions \
   -XX:+UseCompressedOops -XX:+UseCompressedClassPointers -XX:+UseCompactObjectHeaders \
   -XX:AOTCache=app.aot \
   -XX:+AllowArchivingWithJavaAgent \
   -Xnoclassgc --enable-preview --add-modules jdk.incubator.vector \
   -classpath $CP
