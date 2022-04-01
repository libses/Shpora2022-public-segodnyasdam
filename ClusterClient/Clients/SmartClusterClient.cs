using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class SmartClusterClient : ClusterClientBase
{
	public SmartClusterClient(string[] replicaAddresses)
		: base(replicaAddresses)
	{
	}

	public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
	{
		var counter = ReplicaAddresses.Length;
		var sw = new Stopwatch();
		HashSet<Task<string>> tasks = new();
		foreach (var uri in ReplicaAddresses)
		{
			var newTimeout = timeout / counter;
			sw.Restart();
			var webRequest = CreateRequest(uri + "?query=" + query);

			Log.InfoFormat($"Processing {webRequest.RequestUri}");

			tasks.Add(ProcessRequestAsync(webRequest));

			var delay = Task.Delay(newTimeout).ContinueWith(x => "");
			tasks.Add(delay);
			var resultTask = await Task.WhenAny(tasks);
			sw.Stop();
			var p = sw.Elapsed;
			counter--;
			timeout = timeout - p;
			tasks.Remove(delay);
			tasks.RemoveWhere(x => x.IsFaulted);
			if (resultTask.IsCompletedSuccessfully && resultTask != delay)
				return resultTask.Result;
		}

		throw new TimeoutException();
	}

	protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
}