import { ParamValue } from "./param-value.model";

export interface WorkloadParamDesc {
    name : string;
    type : string;
    required : boolean;
    minValue : number;
    maxValue : number;
    defaultValue : ParamValue;
}