import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { YBServerModel } from '../model/yb-server-model.model';
import { TimingData } from '../model/timing-data.model';
import { WorkloadDesc } from '../model/workload-desc.model';
import { ParamValue } from '../model/param-value.model';
import { InvocationResult } from '../model/invocation-result.model';

const PROTOCOL = 'http';
const PORT = 8080;

@Injectable({
  providedIn: 'root'
})
export class YugabyteDataSourceService {
  baseUrl : string;
  testEnv = true;
  constructor(private http: HttpClient) {
    if (this.testEnv) {
      this.baseUrl = `${PROTOCOL}://localhost:${PORT}/`;
    }
    else {
      this.baseUrl = "";
    }
  }

  getServerNodes() :Observable<YBServerModel[]> {
    if (this.testEnv) {
      return this.http.get<YBServerModel[]>(this.baseUrl + "api/ybserverinfo");
    }
    else {
      return this.http.get<YBServerModel[]>(this.baseUrl + "api/ybserverinfo");
    }
  }

  getTimingResults(afterTime : number) : Observable<TimingData> {
    return this.http.get<TimingData>(this.baseUrl + "api/getResults/" + afterTime);
  }

  createTables() : Observable<number> {
    return this.http.get<number>(this.baseUrl + "api/create-table");
  }

  truncateTables() : Observable<number> {
    return this.http.get<number>(this.baseUrl + "api/truncate-table");
  }

  startUpdateWorkload(numThreads : number, numRequests : number) {
    return this.http.get<number>(this.baseUrl + 'api/simulate-updates/' + numThreads + '/' + numRequests);
  }

  startStatusChecksWorkload(numThreads : number, numRequests : number) {
    return this.http.get<number>(this.baseUrl + 'api/simulate-workload2/' + numThreads + '/' + numRequests);
  }

  startSubmissionsWorkload(numThreads : number, numRequests : number) {
    return this.http.get<number>(this.baseUrl + 'api/simulate-workload1/' + numThreads + '/' + numRequests);
  }

  //// Generic interface
  getWorkloads() : Observable<WorkloadDesc[]> {
    return this.http.get<WorkloadDesc[]>(this.baseUrl + 'api/get-workloads');
  }

  invokeWorkload(name : String, params : ParamValue[]) : Observable<InvocationResult> {
    return this.http.post<InvocationResult>(this.baseUrl+"api/invoke-workload/" + name, params);
  }

}
