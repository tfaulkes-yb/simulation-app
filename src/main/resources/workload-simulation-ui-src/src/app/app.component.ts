import { Component, EventEmitter, HostListener } from '@angular/core';
import { TimingData } from './model/timing-data.model';
import { TimingPoint } from './model/timing-point.model';
import { WorkloadDesc } from './model/workload-desc.model';
import { YugabyteDataSourceService } from './services/yugabyte-data-source.service';
import { ParamValue } from './model/param-value.model';
import { WorkloadService } from './services/workload-service.service';
import { WorkloadParamDesc } from './model/workload-param-desc.model';
import { InvocationResult } from './model/invocation-result.model';
import { WorkloadStatus } from './model/workload-status.model';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})

export class AppComponent {
  status = "This is a test messsage";
  updateThreads : number = 20;
  updateRequests : number = 10000;
  workload2Threads : number = 10;
  workload2Requests : number = 40000;
  workload1Threads: number = 5;
  workload1Requests : number = 50000;

  showDialog = false;
  title = 'workload-simulation-ui-src';
  currentData : TimingData = {WORKLOAD2 : [], WORKLOAD1 : []};
  startTime = 0;
  MAX_READINGS = 3600;
  WORKLOAD1 = "WORKLOAD1";
  WORKLOAD2 = "WORKLOAD2";
  LATENCY = "LATENCY";
  THROUGHPUT = "THROUGHPUT";

  workload1Latency = "Workload 1";
  workload2Latency = "Workload 2";
  workload1Throughput = "Workload 1";
  workload2Throughput = "Workload 2";

  workloadValues : any = null;
  valuesComputed = false;
  selected : boolean[] = [];

  activeLoading : boolean = false;
  workloadStatuses : WorkloadStatus[] = [];

  private minDuration = 60*1000;
  private maxDuration = this.MAX_READINGS * 1000;
  duration = 3 * 60 * 1000;

  constructor(private dataSource : YugabyteDataSourceService,
            private workloadService : WorkloadService ) {
    setInterval(() => {
      this.getResults();
    },340);

    workloadService.getWorkloadObservable().subscribe( data => this.computeWorkloadValues(data));
    // this.workloadStatuses.push({name:'testWorkload', startTime:0, endTime:0, status:'Running'});
    // this.workloadStatuses.push({name:'randomWorklad', startTime:0, endTime:0, status:'Running'});
    // this.workloadStatuses.push({name:'seedWorkload', startTime:0, endTime:0, status:'Running'});
    // this.workloadStatuses.push({name:'playWorkload', startTime:0, endTime:0, status:'Running'});
  }

  computeWorkloadValues(workloads : WorkloadDesc[]) {
    // let workloads = this.workloadService.getWorkloads();
    this.workloadValues = {};
    for (let i = 0; i < workloads.length; i++) {
      let thisWorkload = workloads[i];
      let currentValues : any = {};
      for (let j = 0; j < thisWorkload.params.length; j++) {
        let thisParam = thisWorkload.params[j];
        switch (thisParam.type) {
          case 'NUMBER':
            if (thisParam.defaultValue) {
              currentValues[thisParam.name] = thisParam.defaultValue.intValue || 0;
            }
            else {
              currentValues[thisParam.name] = 0;
            }
            break;

          case 'BOOLEAN':
            if (thisParam.defaultValue) {
              currentValues[thisParam.name] = thisParam.defaultValue.boolValue || false;
            }
            else {
              currentValues[thisParam.name] = false;
            }
            break;

          case 'STRING':
            if (thisParam.defaultValue) {
              currentValues[thisParam.name] = thisParam.defaultValue.stringValue || false;
            }
            else {
              currentValues[thisParam.name] = '';
            }
            break;
  
        }
      }
      this.workloadValues[thisWorkload.workloadId] = currentValues;
    }
    this.valuesComputed = true;
  }

  handleChange(e : any) {
    console.log(e.index);
    if (e.index == 1) {
      this.activeLoading = true;
      this.dataSource.getActiveWorkloads().subscribe(workloads => {
        this.activeLoading = false;
        this.workloadStatuses = workloads;
      });
    }
  }

  getWorkloads() {
    return this.workloadService.getWorkloads();
  }

  terminateTask(workloadId : string) {
    this.dataSource.terminateWorkload(workloadId).subscribe( result => {
      this.activeLoading = true;
      this.dataSource.getActiveWorkloads().subscribe(workloads => {
        this.activeLoading = false;
        this.workloadStatuses = workloads;
      });
    });
  }


  private valueToParam(paramDesc: WorkloadParamDesc, paramValue : any) : ParamValue {
    let paramToSend : ParamValue = {type: paramDesc.type};
    switch (paramDesc.type) {
      case 'NUMBER': paramToSend.intValue = paramValue; return paramToSend;
      case 'BOOLEAN': paramToSend.boolValue = paramValue; return paramToSend;
      case 'STRING': paramToSend.stringValue = paramValue; return paramToSend;
    }
    console.log('Unknown parameter type for ' + paramDesc.name);
    return paramToSend;
  }

  launchWorkload(name : String) {
    console.log("launching " + name);
    let paramsToSend : ParamValue[] = [];
    let values = this.workloadValues[name as any];
    let workloads = this.workloadService.getWorkloads();
    for (let i = 0; i < workloads.length; i++) {
      if (workloads[i].workloadId === name) {
        let thisWorkload = workloads[i];

        // Set the names of the workloads
        this.workload1Latency = thisWorkload.workloadNames.WORKLOAD1 || 'Workload 1';
        this.workload2Latency = thisWorkload.workloadNames.WORKLOAD2 || 'Workload 2';
        this.workload1Throughput = thisWorkload.workloadNames.WORKLOAD1 || 'Workload 1';
        this.workload2Throughput = thisWorkload.workloadNames.WORKLOAD2 || 'Workload 2';

        for (let paramIndex = 0; paramIndex < thisWorkload.params.length; paramIndex++) {
          let thisParam = thisWorkload.params[paramIndex];
          let paramName = thisParam.name;
          let thisParamValue = this.valueToParam(thisParam, values[paramName]);
          paramsToSend.push(thisParamValue);
        }
      }
    }
    
    this.status = "Submitting workload " + name + "..."
    this.dataSource.invokeWorkload(name, paramsToSend).subscribe(success => {
      console.log(success);
      if (success.result ==0) {
        this.status = "Workload " + name + " successfully submitted."
      }
      else {
        this.status = "Workload " + name + " failed to submit. Reported error was " + success.data;
      }
    },
    (error => {
      console.log(error);
      this.status = "Workload " + name + " failed to submit";
    }));
  }

  getResults() {
    this.dataSource.getTimingResults(this.startTime).subscribe(data => {
      if (!data.WORKLOAD2) {
        return;
      }

      let newData : TimingData = {WORKLOAD2:[], WORKLOAD1:[]};
      newData.WORKLOAD2 = data.WORKLOAD2.map((value) =>
        {
          return {
            avgUs : value.avgUs / 1000.0,
            maxUs : value.maxUs / 1000.0,
            minUs : value.minUs / 1000.0,
            numFailed: value.numFailed,
            numSucceeded: value.numSucceeded,
            startTimeMs: value.startTimeMs
          } as TimingPoint
        });
      newData.WORKLOAD1 = data.WORKLOAD1.map((value) =>
        {
          return {
            avgUs : value.avgUs / 1000.0,
            maxUs : value.maxUs / 1000.0,
            minUs : value.minUs / 1000.0,
            numFailed: value.numFailed,
            numSucceeded: value.numSucceeded,
            startTimeMs: value.startTimeMs
          } as TimingPoint
        });
      if (this.startTime == 0) {
        this.currentData = newData;
      }
      else {
        // Append these results to the existing data and trim the front if needed
        this.currentData.WORKLOAD2 = this.currentData.WORKLOAD2.concat(newData.WORKLOAD2);
        if (this.currentData.WORKLOAD2.length > this.MAX_READINGS) {
          this.currentData.WORKLOAD2.splice(0, this.currentData.WORKLOAD2.length-this.MAX_READINGS);
        }
        this.currentData.WORKLOAD1 = this.currentData.WORKLOAD1.concat(newData.WORKLOAD1);
        if (this.currentData.WORKLOAD1.length > this.MAX_READINGS) {
          this.currentData.WORKLOAD1.splice(0, this.currentData.WORKLOAD1.length-this.MAX_READINGS);
        }
      }
      if (this.currentData.WORKLOAD2.length > 0) {
        this.startTime = this.currentData.WORKLOAD2[this.currentData.WORKLOAD2.length-1].startTimeMs;
      }
      let temp = this.currentData;
      this.currentData = {WORKLOAD2: temp.WORKLOAD2, WORKLOAD1: temp.WORKLOAD1 };
    });
  }

  @HostListener('wheel', ['$event'])
  onMouseWheel(event : any) {
    if (event.srcElement.closest('p-dialog') == null) {
      event.preventDefault();
      let amount = event.wheelDelta;
      let change = 1+(amount/1200);
      this.duration = Math.floor(Math.max(this.minDuration, Math.min(this.maxDuration, this.duration * change)));
    }
  }

  displayDialog() {
    this.status = "";
    let count = this.getWorkloads().length;
    for (let i =0; i < count; i++) {
      this.selected[i] = false;
    }
    this.showDialog = true;
    // this.computeWorkloadValues(this.workloadService.getWorkloads());
  }

  closeDialog() {
    this.showDialog = false;
  }

  createTables() {
    this.status = "Creating tables...";
    this.dataSource.createTables().subscribe(data => {
      this.status = "Table creation complete.";
    },
    error => {
      this.status = "Table creation failed."
    });
  }

  truncateTables() {
    this.status = "Truncating tables...";
    this.dataSource.truncateTables().subscribe(data => {
      this.status = "Table truncation complete.";
    },
    error => {
      this.status = "Table truncation failed."
    });
  }

  startUpdateWorkload(numThreads : number, numRequests : number) {
    this.status = "Update workload starting...";
    this.dataSource.startUpdateWorkload(numThreads, numRequests).subscribe(data => {
      this.status = "Update workload complete";
    },
    error => {
      this.status = "Update workload failed."
    });
    this.status = "Update workload started.";
  }

  startStatusCheckWorkload(numThreads : number, numRequests : number) {
    this.status = "Status check workload starting...";
    this.dataSource.startStatusChecksWorkload(numThreads, numRequests).subscribe(data => {
      this.status = "Status check workload complete";
    },
    error => {
      this.status = "Status check workload failed."
    });
    this.status = "Status check workload started.";
  }

  startSubmissionsWorkload(numThreads : number, numRequests : number) {
    this.status = "Submissions workload starting...";
    this.dataSource.startSubmissionsWorkload(numThreads, numRequests).subscribe(data => {
      this.status = "Submissions workload complete";
    },
    error => {
      this.status = "Submissions workload failed."
    });
    this.status = "Submissions workload started.";
  }


}
