{{/*
Expand the name of the chart.
*/}}
{{- define "pgcat.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "pgcat.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "pgcat.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "pgcat.labels" -}}
helm.sh/chart: {{ include "pgcat.chart" . }}
{{ include "pgcat.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "pgcat.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pgcat.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "pgcat.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "pgcat.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Defines a password function which will assign the appropriate password to the supplied key.

It will use the literal value from `.password` if it is present. Otherwise it will fetch the value from the
specified secret and use that.

If the password is blank, and the secret object does not contain both name and key properties this returns `""`.
Similarly, if the secret lookup fails, this also returns `""`.

NB: For this lookup to succeed, the secret must already be defined. Notably this means that it's not likely to be
managed directly by this chart. It also means that changes to the secret require an upgrade of the release, since the
value of the secret is effectively copied into this manifest.

Args:
  * password = The plaintext password
  * secret = An object (key and name) to use as essentially as a secretKeyRef
*/}}
{{- define "pgcat.password" -}}
{{- if .password }}
{{- .password | quote }}
{{- else if and .secret.name .secret.key }}
{{- $secret := (lookup "v1" "Secret" $.Release.Namespace .secret.name) }}
{{- if $secret }}
{{- $password := index $secret.data .secret.key | b64dec }}
{{- $password | quote }}
{{- else }}
""
{{- end }}
{{- end }}
{{- end -}}
