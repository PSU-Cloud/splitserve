package example;

import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.lang.*;
import java.io.*;
import java.util.Arrays;

public class SplitServe implements RequestHandler<RequestClass, ResponseClass>{   
    public ResponseClass handleRequest(RequestClass request, Context context){
        String greetingString = String.format("Hello %s, %s, %s, %s, %s.", request.sparkDriverHostname, request.sparkDriverPort, request.sparkCommandLine, request.javaPartialCommandLine, request.executorPartialCommandLine);

    System.out.println("This is sample output ");

    String spark_driver_hostname = request.getSparkDriverHostname();
    String spark_driver_port = request.getSparkDriverPort();
    String spark_executor_cmdline = request.getSparkCommandLine();
    String java_partial_cmdline = request.getJavaPartialCommandLine();
    String executor_partial_cmdline = request.getExecutorPartialCommandLine();
    String java_extra_options = "-Dspark.lambda.awsRequestId=" + context.getAwsRequestId() + "  " + 
        "-Dspark.lambda.logGroupName=" + context.getLogGroupName() + " " + 
        "-Dspark.lambda.logStreamName=" + context.getLogStreamName() + " ";

    System.out.println("Hostname = " + request.sparkDriverHostname);
    System.out.println("Port = " + request.sparkDriverPort);
    System.out.println("Java cmdline = " + request.javaPartialCommandLine);

    String cmdline = java_partial_cmdline + java_extra_options + executor_partial_cmdline;
    String[] cmdline_arr = cmdline.split(" ", 0);
    String[] cmdToCall = Arrays.stream(cmdline_arr)
                .filter(value ->
                        value != null && value.length() > 0
                )
                .toArray(size -> new String[size]);
   
    System.out.println("START: SplitServe executor: " + cmdToCall.toString());
	
    try {
    	Process executor = new ProcessBuilder().command(cmdToCall).inheritIO().start();
    	int exitCode = executor.waitFor();
        System.out.println("exitCode = " + exitCode);
    } catch (Exception e) {
        System.out.println("Exception = " + e);
    }

    System.out.println("FINISH: SplitServe executor");

    return new ResponseClass("Success");
    }
}
