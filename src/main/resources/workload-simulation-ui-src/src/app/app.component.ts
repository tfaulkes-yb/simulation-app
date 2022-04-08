import { Component, EventEmitter, HostListener } from '@angular/core';
import { TimingData } from './model/timing-data.model';
import { TimingPoint } from './model/timing-point.model';
import { YugabyteDataSourceService } from './services/yugabyte-data-source.service';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})

export class AppComponent {
  status = "This is a test messsage";
  updateThreads : number = 20;
  updateRequests : number = 10000;
  statusCheckThreads : number = 10;
  statusCheckRequests : number = 40000;
  submissionThreads: number = 5;
  submissionRequests : number = 50000;

  showDialog = false;
  title = 'workload-simulation-ui-src';
  currentData : TimingData = {STATUS : [], SUBMISSION : []};
  startTime = 0;
  MAX_READINGS = 3600;
  SUBMISSION = "SUBMISSION";
  STATUS = "STATUS";
  LATENCY = "LATENCY";
  THROUGHPUT = "THROUGHPUT";

  private minDuration = 60*1000;
  private maxDuration = this.MAX_READINGS * 1000;
  duration = 3 * 60 * 1000;

  constructor(private dataSource : YugabyteDataSourceService ) {
    setInterval(() => {
      this.getResults();
    },350);
  }

  getResults() {
    this.dataSource.getTimingResults(this.startTime).subscribe(data => {
      if (!data.STATUS) {
        return;
      }

      let newData : TimingData = {STATUS:[], SUBMISSION:[]};
      newData.STATUS = data.STATUS.map((value) =>
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
      newData.SUBMISSION = data.SUBMISSION.map((value) =>
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
        this.currentData.STATUS = this.currentData.STATUS.concat(newData.STATUS);
        if (this.currentData.STATUS.length > this.MAX_READINGS) {
          this.currentData.STATUS.splice(0, this.currentData.STATUS.length-this.MAX_READINGS);
        }
        this.currentData.SUBMISSION = this.currentData.SUBMISSION.concat(newData.SUBMISSION);
        if (this.currentData.SUBMISSION.length > this.MAX_READINGS) {
          this.currentData.SUBMISSION.splice(0, this.currentData.SUBMISSION.length-this.MAX_READINGS);
        }
      }
      if (this.currentData.STATUS.length > 0) {
        this.startTime = this.currentData.STATUS[this.currentData.STATUS.length-1].startTimeMs;
      }
      let temp = this.currentData;
      this.currentData = {STATUS: temp.STATUS, SUBMISSION: temp.SUBMISSION };
    });
  }

  @HostListener('wheel', ['$event'])
  onMouseWheel(event : any) {
    event.preventDefault();
    let amount = event.wheelDelta;
    let change = 1+(amount/1200);
    this.duration = Math.floor(Math.max(this.minDuration, Math.min(this.maxDuration, this.duration * change)));
  }

  displayDialog() {
    this.status = "";
    this.showDialog = true;
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
