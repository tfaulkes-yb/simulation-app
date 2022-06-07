import { CUSTOM_ELEMENTS_SCHEMA, NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { AppComponent } from './app.component';
import { ThroughputComponent } from './components/throughput/throughput.component';
import { StatisticsGraphComponent } from './components/statistics-graph/statistics-graph.component';
import { HttpClientModule } from '@angular/common/http';
import { NetworkDiagramComponent } from './components/network-diagram/network-diagram.component';
import { AccordionModule } from 'primeng/accordion';
import { ButtonModule } from 'primeng/button';
import { DialogModule } from 'primeng/dialog';
import { InputNumberModule } from 'primeng/inputnumber';
import { InputSwitchModule } from 'primeng/inputswitch';
import { InputTextModule } from 'primeng/inputtext';
import { MenuModule } from 'primeng/menu'
import { PanelModule } from 'primeng/panel';
import { SliderModule} from 'primeng/slider';
import { StepsModule } from 'primeng/steps'
import { FormsModule } from '@angular/forms';
import { TableModule } from 'primeng/table';
import { TabViewModule } from 'primeng/tabview';

import { WorkloadService } from './services/workload-service.service';
import { StepsDiagramComponent } from './components/steps-diagram/steps-diagram.component';
import { RouterModule } from '@angular/router';

@NgModule({
  declarations: [
    AppComponent,
    ThroughputComponent,
    StatisticsGraphComponent,
    NetworkDiagramComponent,
    StepsDiagramComponent,
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
    MenuModule,
    PanelModule,
    SliderModule,
    StepsModule,
    FormsModule,
    TabViewModule,
    TableModule,
    RouterModule.forRoot([
      {path:'', component: AppComponent}
    ])
  ],
  providers: [WorkloadService],
  bootstrap: [AppComponent],
  schemas: [CUSTOM_ELEMENTS_SCHEMA]
})
export class AppModule { }
