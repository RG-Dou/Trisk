// same as p12.c, with documentation 
// input: a file with 2 columns of M (1st column) and P values (2nd column)
// output: 4 parameters, R2 and gnuplot script
// R2 value is from linear regression fit of <x,M> values (excludes tail)

// algorithm: finds best possible Pstar value from 2 cases
// Case(a): assume there is a tail and find Pstar by locating the body
// Case(b): assume there is no tail and find Pstar by searching from minP down
// pick best Pstar using minSSE (minimum sum of square errors) for body+tail

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

int FALSE=0, TRUE=1;
int PSTAR=0, PZERO=1, MZERO=2, MSTAR=3, RSQUARE=4;

main(int argc, char **argv)
{
  float M[10000], P[10000], Pstar;
  float PFE[5];

  void geteqn(float M[], float P[], int num_points, 
              float minM, float maxM, float minP, float maxP, float PFE[]);
  void GenScript(char input_file[], int Mrange, float Prange, float PFE[]);

  if (argc!=2) {
                printf("usage: fit data_file \n");
                return 0;
  }

  FILE *fp = fopen(argv[1], "r");

  int i = 0;
  while(fscanf(fp, "%f\t%f\n", &M[i], &P[i])!=EOF) i++;

  int   num_points=i;

  float minM = M[0];
  float maxM = M[0];
  float minP = P[0];
  float maxP = P[0];

  for(i=0; i<num_points; i++){
      if(M[i]<minM) minM = M[i];
      if(M[i]>maxM) maxM = M[i];
      if(P[i]<minP) minP = P[i];
      if(P[i]>maxP) maxP = P[i];
  }

  geteqn(M, P, num_points, minM, maxM, minP, maxP, PFE);

  char input_file[128];
  strcpy(input_file, argv[1]);
  GenScript(input_file, 1.2*maxM, 1.2*maxP, PFE);

}


void geteqn(float M[], float P[], int num_points, 
            float minM, float maxM, float minP, float maxP, float PFE[])
{
  void GetEqnWithPstar(float M[], float P[], float Pstar, int body, int n, float PFE[]);
  float ComputeSSE(float M[], float P[], int num_points, float PFE[]);
  float aveP(float P[], int body, int num_points);

                   //Case(a): find Pstar by extending the tail from num_points
  float a_SSE, a_Pstar, best_a_Pstar; 
  float a_minSSE   = (float) num_points; //works since P[i]<1
  float a_feasible = FALSE;
  int   a_body, best_a_body;

  for(a_body=num_points-1; a_body>5; a_body--){
      a_Pstar = aveP(P, a_body, num_points);   //candidate = average the tail 
      GetEqnWithPstar(M, P, a_Pstar, a_body, num_points, PFE);
      if(PFE[MZERO]<minM && a_Pstar<minP+0.05*(maxP-minP)){
                            //discard if M0 is illegal or tail is too fat
         a_feasible = TRUE;
         a_SSE = ComputeSSE(M, P, num_points, PFE);
         if(a_SSE < a_minSSE){
            a_minSSE     = a_SSE;
            best_a_body  = a_body;
            best_a_Pstar = a_Pstar;
         }
      }
  printf("a_minSSE=%f best_a_body=%d best_a_Pstar=%f\n", a_minSSE, best_a_body, best_a_Pstar);
  }

                   //Case(b): find Pstar by searching from minP downwards
  float b_SSE, b_Pstar, best_b_Pstar;
  float b_minSSE   = (float) num_points; 
  float b_feasible = FALSE;
  int   b_body = num_points-1;
  int minP_divisor = 10;            //number of tries

  best_b_Pstar = minP;              //default value
  for(int i=minP_divisor; i>0; i--){
      b_Pstar = i*minP/minP_divisor;
      GetEqnWithPstar(M, P, b_Pstar, b_body, num_points, PFE);
      if(PFE[MZERO]<minM && PFE[MSTAR]<3*maxM){ 
                           //discard if M0 is illegal or tail is too long
         b_feasible = TRUE;
         b_SSE = ComputeSSE(M, P, num_points, PFE);
         if(b_SSE < b_minSSE){
            b_minSSE     = b_SSE;
            best_b_Pstar = b_Pstar;
         }
      }
   printf("b_minSSE=%f b_body=%d best_b_Pstar=%f\n", b_minSSE, b_body, best_b_Pstar);
  }
                                 //pick candidate Pstar with minimum SSE 
  if(a_feasible){
     if(b_feasible){
        if(a_minSSE < b_minSSE){
           GetEqnWithPstar(M, P, best_a_Pstar, best_a_body, num_points, PFE);
        }
        else GetEqnWithPstar(M, P, best_b_Pstar, b_body, num_points, PFE);
     }
     else GetEqnWithPstar(M, P, best_a_Pstar, best_a_body, num_points, PFE);
  }
  else{
     if(b_feasible){
        GetEqnWithPstar(M, P, best_b_Pstar, b_body, num_points, PFE);
     }
     else{ printf("no feasible fit?\n"); exit; }
  } 

}


void GetEqnWithPstar(float M[], float P[], float Pstar, int body, int n, float PFE[])
{                    //for this Pstar, find best P0 by R2 value from regression
  float P0, previousPFE[5];

  void GetEqnWithP0(float M[], float P[], float P0, float Pstar, int body, int n, float PFE[]);
  void arraycpy(float to[], float from[], int size);

  if(Pstar==0){ printf("Pstar==0?\n"); exit; }

  float deltaP0       = Pstar/100;  //baby steps to find best P0

  previousPFE[PSTAR]    =  Pstar;
  previousPFE[PZERO]    = -Pstar;   //start P0 search from -Pstar
  previousPFE[MZERO]    = 0.0;
  previousPFE[MSTAR]    = M[0];
  previousPFE[RSQUARE]  = 0.0;
  
  int   foundP0 = FALSE;
  while(foundP0== FALSE){
        P0 = previousPFE[PZERO] + deltaP0;
        if(Pstar+P0 > 1){ 
//         printf("Pstar+P0 > 1?\n"); 
        exit; 
        }

        GetEqnWithP0(M, P, P0, Pstar, body, n, PFE);

//      printf("P0=%f Pstar=%f body=%d n=%d R2=%f\n\n", 
//              PFE[PZERO], PFE[PSTAR], body, n, PFE[RSQUARE]);

        if(PFE[RSQUARE] > previousPFE[RSQUARE]){
           arraycpy(previousPFE, PFE, 5);
           previousPFE[PZERO] = P0;
         }
         else{
              arraycpy(PFE, previousPFE, 5);
              foundP0 = TRUE;
         }
   }

}



void GetEqnWithP0(float M[], float P[], float P0, float Pstar, int body, int n, float PFE[])
{           //for fixed Pstar and fixed P0, do regression and record R2 value 
  int   i; 
  float x[10000], f[10000], intercept_slope[3], sumP;
  void lregress(float x[], float M[], int body, float intercept_slope[]);
 
  for(i=0; i<body; i++){                 //regression excludes the tail
      float r = (P[i] + P0)/(Pstar + P0);
         x[i] = 1/(r - 1 + 1/r);
  }

  lregress(x, M, body, intercept_slope);
  float M0     = intercept_slope[0];                      //M0=intercept
  float Mstar  = intercept_slope[0] + intercept_slope[1]; //M*=intercept+slope
  float bodyR2 = intercept_slope[2];
 

  PFE[PSTAR] = Pstar;
  PFE[PZERO] = P0;
  PFE[MZERO] = M0;
  PFE[MSTAR] = Mstar;
  PFE[RSQUARE] = bodyR2;                 //R2 excludes the tail

}


void lregress(float X[], float Y[], int num_points, float intercept_slope[])
{
  float sumX, sumY, sumX2, sumY2, sumXY; 
  float intercept, slope;

  sumX = sumY = sumX2 = sumY2 = sumXY = 0.0;
 
  for(int i=0; i<num_points; i++){
      sumX = sumX + X[i];
      sumY = sumY + Y[i];
      sumX2 = sumX2 + X[i]*X[i];
      sumY2 = sumY2 + Y[i]*Y[i];
      sumXY = sumXY + X[i]*Y[i];
  }

  slope     = (num_points*sumXY - sumX*sumY)/(num_points*sumX2 - sumX*sumX);
  intercept = (sumY - slope*sumX)/num_points;

  intercept_slope[0] = intercept;
  intercept_slope[1] = slope;
  intercept_slope[2] = (intercept*sumY + slope*sumXY - sumY*sumY/num_points) 
                      /(sumY2 - sumY*sumY/num_points);

}


void GenScript(char input_file[], int Mrange, float Prange, float PFE[])
{
  printf("Mstar=%f  M0=%f  P*=%f  P0=%f  R2=%f\n",
          PFE[MSTAR], PFE[MZERO], PFE[PSTAR], PFE[PZERO], PFE[RSQUARE]);

  char out_file[128], eps_file[128];
  strcpy(eps_file, "\"");
  strcat(eps_file, input_file);
  strcat(eps_file, ".p12.eps\"");
  strcpy(out_file, input_file);
  strcat(out_file, ".g");
  
  FILE *fp = fopen(out_file, "w");
  fprintf(fp, "set term postscript eps enhanced\n");
  fprintf(fp, "set output %s\n", eps_file);
  fprintf(fp, "set xrange [0:%d]\n", (int) Mrange);
  fprintf(fp, "set yrange [0:%.1f]\n", Prange);
  fprintf(fp, "set xlabel 'memory size' font \"Verdana,20\"\n");
  fprintf(fp, "set ylabel 'miss probability' font \"Verdana,20\"\n");

  float Pstar = PFE[PSTAR];
  float P0    = PFE[PZERO];
  float M0    = PFE[MZERO];
  float Mstar = PFE[MSTAR];
  float R2    = PFE[RSQUARE];
  fprintf(fp, "Mstar=%f\n", Mstar);
  fprintf(fp, "M0=%f\n", M0);
  fprintf(fp, "Pstar=%f\n", Pstar);
  fprintf(fp, "P0=%f\n", P0);

  fprintf(fp, "set title \"M*=%.0f M0=%.0f P*=%.4f P0=%.4f R2=%.4f\" font \"Verdana,20\"\n", Mstar, M0, Pstar, P0, R2);

  fprintf(fp, "P(x) = (x < M0) ? 1 : (x < Mstar) ? 0.5*(1+(Mstar-M0)/(x-M0)+sqrt((1+(Mstar-M0)/(x-M0))*(1+(Mstar-M0)/(x-M0))-4))*(Pstar + P0) - P0 : Pstar\n");

  fprintf(fp, "plot '%s' using 1:2 with points, P(x) title 'equation' with lines\n", input_file);

}  


void arraycpy(float to[], float from[], int size)
{
  for(int i=0; i<size; i++) to[i] = from[i];
}


float ComputeSSE(float M[], float P[], int num_points, float PFE[])
{
  float  sse, pfe[10000];

  if(PFE[MZERO]>M[0]){ printf("M0 > M[0]?\n"); exit; }

  float Mstar=PFE[MSTAR];
  float Pstar=PFE[PSTAR];
  float M0   =PFE[MZERO];
  float P0   =PFE[PZERO];

  for(int i=0; i<num_points; i++){
      if(M[i]<Mstar){
         pfe[i] = 0.5*(1+(Mstar-M0)/(M[i]-M0) +sqrt((1+(Mstar-M0)/(M[i]-M0))*(1+
(Mstar-M0)/(M[i]-M0))-4))*(Pstar + P0) - P0;
      }
      else pfe[i] = Pstar;
  }

  sse = 0.0;
  for(int i=0; i<num_points; i++){
      sse = sse + (P[i]-pfe[i])*(P[i]-pfe[i]);
//  printf("P[%d]=%f pfe[%d]=%f\n", i, P[i], i, pfe[i]);
  }
  return sse;

}


float aveP(float P[], int body, int num_points)
{
  float sumP=00;

  for(int i=body; i<num_points; i++){
      sumP = sumP + P[i];
  }
  return sumP/(num_points - body);

}
  
