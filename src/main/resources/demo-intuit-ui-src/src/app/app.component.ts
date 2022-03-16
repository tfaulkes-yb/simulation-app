import { TransitiveCompileNgModuleMetadata } from '@angular/compiler';
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
  title = 'demo-intuit-ui-src';
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
}
