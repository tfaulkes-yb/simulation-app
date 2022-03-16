import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent } from './app.component';
import { ThroughputComponent } from './components/throughput/throughput.component';
import { HttpClientModule } from '@angular/common/http';
import { NetworkDiagramComponent } from './components/network-diagram/network-diagram.component';

@NgModule({
  declarations: [
    AppComponent,
    ThroughputComponent,
    NetworkDiagramComponent
  ],
  imports: [
    BrowserModule,
    HttpClientModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
