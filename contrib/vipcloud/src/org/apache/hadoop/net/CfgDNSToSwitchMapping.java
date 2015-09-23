package org.apache.hadoop.net;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class CfgDNSToSwitchMapping implements DNSToSwitchMapping, Configurable
{
	private static Log LOG = LogFactory.getLog(CfgDNSToSwitchMapping.class);
	static final String TOPOLOGY_CONFIG_FILE_PATH = "topology.config.file.path";
	private Configuration conf;
	protected String configFilePath;
	protected long configFileLastModifyTime = -1;
	protected Map<String, String> mapping = new HashMap<String, String>();

	protected void CachDNSConfig(File file)
	{
		synchronized (mapping)
		{
			mapping.clear();
			try
			{
				if (!file.exists())
				{
					return;
				}
				FileInputStream fis = new FileInputStream(file);
				BufferedReader reader = null;
				try
				{
					reader = new BufferedReader(new InputStreamReader(fis));
					String line;
					while ((line = reader.readLine()) != null)
					{
						String[] item = line.split("[ \t\n\f\r]+");
						if (item != null)
						{
							if (item.length >= 2)
							{
								LOG.info("rockInfo " + item[0] + " to "
										+ item[1]);
								mapping.put(item[0], item[1]);
							}
						}
					}
					configFileLastModifyTime = file.lastModified();  
				}
				finally
				{
					if (reader != null)
					{
						reader.close();
					}
					fis.close();
				}
			}
			catch (IOException e)
			{
				LOG.info("read rock config error!", e);
			}
		}
	}

	@Override
	public List<String> resolve(List<String> names)
	{
		names = NetUtils.normalizeHostNames(names);
		List<String> m = new ArrayList<String>(names.size());
		if (names.isEmpty())
		{
			return m;
		}
		
		//判断文件是否有修改,如果有修改重新载入配置
		File file = new File(configFilePath);
		if(file.exists())
		{
			if(configFileLastModifyTime != file.lastModified())
			{
				CachDNSConfig(file);
			}
		}
		
		synchronized(mapping)
		{
			for (String name : names)
			{
				String networkLocation = mapping.get(name);
				if (networkLocation != null)
				{
					LOG.info("resolve name" + name + " to " + networkLocation);
					m.add(networkLocation);
				}
				else
				{
					LOG.warn("name = " + name + " no config networkLocation.");
					return null;
				}
			}
		}
		return m;
	}

	@Override
	public void setConf(Configuration conf)
	{
		this.configFilePath = conf.get(TOPOLOGY_CONFIG_FILE_PATH,
				"topology.cfg");
		this.conf = conf;
		CachDNSConfig(new File(configFilePath));
	}

	@Override
	public Configuration getConf()
	{
		return this.conf;
	}
}
