import { CUSTOM_ELEMENTS_SCHEMA, NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { AppComponent } from './app.component';
import { ThroughputComponent } from './components/throughput/throughput.component';
import { HttpClientModule } from '@angular/common/http';
import { NetworkDiagramComponent } from './components/network-diagram/network-diagram.component';
import { AccordionModule } from 'primeng/accordion';
import { ButtonModule } from 'primeng/button';
import { DialogModule } from 'primeng/dialog';
import { InputNumberModule } from 'primeng/inputnumber';
import { InputSwitchModule } from 'primeng/inputswitch';
import { InputTextModule } from 'primeng/inputtext';
import { SliderModule} from 'primeng/slider';
import { FormsModule } from '@angular/forms';
import { TableModule } from 'primeng/table';
import { TabViewModule } from 'primeng/tabview';

import { WorkloadService } from './services/workload-service.service';

@NgModule({
  declarations: [
    AppComponent,
    ThroughputComponent,
    NetworkDiagramComponent,
  ],
  imports: [
    AccordionModule,
    BrowserModule,
    BrowserAnimationsModule,
    ButtonModule,
    DialogModule,
    HttpClientModule,
    InputNumberModule,
    InputSwitchModule,
    InputTextModule,
    SliderModule,
    FormsModule,
    TabViewModule,
    TableModule
  ],
  providers: [WorkloadService],
  bootstrap: [AppComponent],
  schemas: [CUSTOM_ELEMENTS_SCHEMA]
})
export class AppModule { }
