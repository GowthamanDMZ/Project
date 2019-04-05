package com.esi.ivr.base.config;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.http.HttpStatus;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.esi.ivr.base.components.SpringContext;
import com.esi.ivr.base.support.base.ConfigDataReader;
import com.esi.ivr.base.util.CustomLogger;
import com.esi.ivr.base.util.ILoggerConstants;
import com.esi.ivr.base.util.JsonUtil;
import com.esi.ivr.base.util.ParamStoreLoader;

@Configuration
@Component
public class LoadCFGService{
	
	@Value("${ivr.cfg.mgr.url}")
	String endPointUrl;
	
	@Value("${ivr.cfg.mgr.connect.timeout}")
	private int connectionTimeout;
	
	@Value("${ivr.cfg.mgr.read.timeout}")
	private int readTimeout;
	
	@Value("${ivr.cfg.mgr.username}")
	private String username;
	
	@Value("${ivr.cfg.mgr.password}")
	private String password;

	@Autowired
	ApplicationConfig appConfig;
	
	private RestTemplate restTemplate;
	
//	private List<Object> alOutput;
	
	public LoadCFGService() {
//		alOutput = new ArrayList<Object>();		
	}
	
	private void initialize(){
		Log(ILoggerConstants.INFO, "LoadCFGService : initialize : endPointUrl : "+endPointUrl);
		Log(ILoggerConstants.INFO, "LoadCFGService : initialize : connectionTimeout : "+connectionTimeout);
		Log(ILoggerConstants.INFO, "LoadCFGService : initialize : readTimeout : "+readTimeout);
		Log(ILoggerConstants.INFO, "LoadCFGService : initialize : username : "+username);
		Log(ILoggerConstants.INFO, "LoadCFGService : initialize : password : *******");
		
		RestTemplateBuilder builder = SpringContext.getApplicationContext().getBean(RestTemplateBuilder.class);
		
		restTemplate = builder.setConnectTimeout(connectionTimeout)
				.setReadTimeout(readTimeout)
				.basicAuthorization(username, password).build();
	}

	@PostConstruct
	public void execute() {

		if (null == restTemplate) {
			initialize();
		}

		HashMap<String, String> requestParams = new HashMap<String, String>();

		// requestParams.put("appName", appConfig.getAppName());

		// Log(ILoggerConstants.DEBUG, "LoadCFGService : execute : requestParams
		// : "+requestParams);

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		List<MediaType> acceptedMediaTypes = new ArrayList<MediaType>();
		acceptedMediaTypes.add(MediaType.APPLICATION_JSON);
		headers.setAccept(acceptedMediaTypes);// (MediaType.APPLICATION_JSON);

		HttpEntity<?> entity = new HttpEntity<Object>(requestParams, headers);

		Log(ILoggerConstants.DEBUG, "LoadCFGService : execute : data : " + entity.getBody());
		String url = endPointUrl + "?appName=" + appConfig.getAppName();
		
		int count = 0;
		boolean gotData = true;
		while (gotData) {
			
			if(count==4){
				gotData=false;
			}
			
			try {
				ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
				int statusCode = response.getStatusCode().value();

				Log(ILoggerConstants.INFO, "LoadCFGService : execute : statusCode : " + statusCode);

				if (statusCode >= HttpStatus.SC_OK && statusCode < HttpStatus.SC_MULTIPLE_CHOICES) {
					try{
						Map<String, Object> cfgMap = transformResponse(response.getBody().toString());
						Log(ILoggerConstants.FINEST, "execute : cfgMap : " + cfgMap);
						ParamStoreLoader psl = (ParamStoreLoader) SpringContext.getApplicationContext()
								.getBean(ParamStoreLoader.class);
						psl.addAppConfigData(cfgMap);
						ConfigDataReader configReader = (ConfigDataReader) SpringContext.getApplicationContext()
								.getBean(ConfigDataReader.class);
						configReader.AddAppConfigData(cfgMap);
					}catch(Exception e){
						Log(ILoggerConstants.ERROR, "Error While transforming data");
					}
				} else {
					Log(ILoggerConstants.FINEST, "LoadCFGService : waiting : 2 Mins : inside else block, statusCode:"+statusCode);
					Thread.sleep(60*2*1000);
					count++;
					continue;
				}
			} catch (RestClientException e) {
				try {
					Log(ILoggerConstants.FINEST, "LoadCFGService : waiting : 2 Mins : RestClientException occured");
					Thread.sleep(60*2*1000);
					count++;
					continue;
				} catch (InterruptedException e1) {
					Log(ILoggerConstants.FINEST, "LoadCFGService : Error while waiting : 2 Mins : InterruptedException occured");
				}
			} catch (InterruptedException inx){
				Log(ILoggerConstants.FINEST, "Interrupted by another thread");
			} catch (Exception e) {
				try {
					Log(ILoggerConstants.FINEST, "LoadCFGService : waiting : 2 Mins : ");
					Thread.sleep(60*2*1000);
					count++;
					continue;
				} catch (InterruptedException e1) {
					Log(ILoggerConstants.FINEST, "LoadCFGService : Error while waiting ");
				}
			}
		}

		Log(ILoggerConstants.INFO, "LoadCFGService : execute : exit");

	}
	
	/**
	 * 
	 * @param response
	 * @return
	 * @throws BackendException
	 */
	private Map<String, Object> transformResponse(String response) {

		Log(ILoggerConstants.FINEST, "transformResponse : response : "+response);
		
		Map<String, Object> hmRespOutput = new HashMap<String, Object> ();

		try {
			String str = "{\"entry\": " + response.toString() + "}";
			JSONObject outTmp = new JSONObject(str);
			JSONArray outArray = (JSONArray) outTmp.get("entry");
			JSONObject outputData = (JSONObject) outArray.get(0);
			
			Log(ILoggerConstants.FINEST, "transformResponse : outputData : "+outputData);
			
			hmRespOutput = JsonUtil.formatHashMap(outputData);
			
			if(null == hmRespOutput){
				throw new Exception("CFG is null !");
			}
			
			Log(ILoggerConstants.FINEST, "transformResponse : hmRespOutput : "+hmRespOutput);			
			Log(ILoggerConstants.INFO, "transformResponse : Total Files : "+hmRespOutput.size());
			Log(ILoggerConstants.INFO, "transformResponse : Files : "+hmRespOutput.keySet());
			
			
		} catch (Exception e) {
			e.printStackTrace();
			Log(ILoggerConstants.WARNING, "transformResponse : error : "+e.getMessage());
		}
		return hmRespOutput;

	}
	
	/**
	 * 
	 * @param level
	 * @param message
	 */
	protected void Log(String level, String message){
		 CustomLogger.debugLog(ILoggerConstants.DEBUG_LOG, level , message);
	}
	
}

