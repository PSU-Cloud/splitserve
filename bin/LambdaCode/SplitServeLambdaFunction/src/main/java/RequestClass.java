package example;
        
public class RequestClass {
        String sparkDriverHostname;
        String sparkDriverPort;
	String sparkCommandLine;
	String javaPartialCommandLine;
	String executorPartialCommandLine;

        public String getSparkDriverHostname() {
            return sparkDriverHostname;
        }

        public void setSparkDriverHostname(String sparkDriverHostname) {
            this.sparkDriverHostname = sparkDriverHostname;
        }

        public String getSparkDriverPort() {
            return sparkDriverPort;
        }

        public void setSparkDriverPort(String sparkDriverPort) {
            this.sparkDriverPort = sparkDriverPort;
        }

	public String getSparkCommandLine() {
	    return sparkCommandLine;
	}

	public void setSparkCommandLine(String sparkCommandLine) {
	    this.sparkCommandLine = sparkCommandLine;
	}

        public String getJavaPartialCommandLine() {
	    return javaPartialCommandLine;
	}

	public void setJavaPartialCommandLine(String javaPartialCommandLine) {
	    this.javaPartialCommandLine = javaPartialCommandLine;
	}

	public String getExecutorPartialCommandLine() {
	    return executorPartialCommandLine;
	}

	public void setExecutorPartialCommandLine(String executorPartialCommandLine) {
	    this.executorPartialCommandLine = executorPartialCommandLine;
	}

        public RequestClass(String sparkDriverHostname, String sparkDriverPort, String sparkCommandLine, 
			    String javaPartialCommandLine, String executorPartialCommandLine) {
            this.sparkDriverHostname = sparkDriverHostname;
            this.sparkDriverPort = sparkDriverPort;
	    this.sparkCommandLine = sparkCommandLine;
	    this.javaPartialCommandLine = javaPartialCommandLine;
	    this.executorPartialCommandLine = executorPartialCommandLine;
        }

        public RequestClass() {
        }
}

