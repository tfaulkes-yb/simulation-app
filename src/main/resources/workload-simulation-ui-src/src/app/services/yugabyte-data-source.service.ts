import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { YBServerModel } from '../model/yb-server-model.model';
import { TimingData } from '../model/timing-data.model';

const PROTOCOL = 'http';
const PORT = 8080;

@Injectable({
  providedIn: 'root'
})
export class YugabyteDataSourceService {
  baseUrl : string;
  testEnv = false;
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

  startUpdateWorkload(numThreads : number, numRequests : number) {
    return this.http.get<number>(this.baseUrl + 'api/simulate-updates/' + numThreads + '/' + numRequests);
  }

  startStatusChecksWorkload(numThreads : number, numRequests : number) {
    return this.http.get<number>(this.baseUrl + 'api/simulate-status-checks/' + numThreads + '/' + numRequests);
  }

  startSubmissionsWorkload(numThreads : number, numRequests : number) {
    return this.http.get<number>(this.baseUrl + 'api/simulate-submissions/' + numThreads + '/' + numRequests);
  }

}
