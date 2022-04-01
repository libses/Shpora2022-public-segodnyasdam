using System;
using System.Diagnostics;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class RoundRobinClusterClient : ClusterClientBase
{
	public RoundRobinClusterClient(string[] replicaAddresses)
		: base(replicaAddresses)
	{
	}

	public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
	{
		var counter = ReplicaAddresses.Length;
		var sw = new Stopwatch();
		foreach (var uri in ReplicaAddresses)
        {
			var newTimeout = timeout / counter;
			sw.Restart();
			var webRequest = CreateRequest(uri + "?query=" + query);

			Log.InfoFormat($"Processing {webRequest.RequestUri}");

			var resultTask = ProcessRequestAsync(webRequest);

			if (resultTask.IsFaulted) continue;

			await Task.WhenAny(resultTask, Task.Delay(newTimeout));
			sw.Stop();
			var p = sw.Elapsed;
			counter--;
			timeout = timeout - p;
			if (resultTask.IsCompletedSuccessfully)
				return resultTask.Result;
		}

		throw new TimeoutException();
	}

	protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
}