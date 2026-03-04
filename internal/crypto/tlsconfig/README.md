# tlsconfig

`tlsconfig` package deals with getting & keeping `tls.Config` up to date as certificates are renewed.

## example

Here we're loading the certificate and key from the default location `/certs` and creating a Kafka client:

```go
tls, err := tlsconfig.New()
if err != nil {
    return nil, fmt.Errorf("failed initialising TLS config: %w", err)
}

cl, err := kgo.NewClient(
    kgo.DialTLSConfig(tls),
)
```

We typically use [cert-manager](https://cert-manager.io/) to create these certificates, for example:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: cerbos-exporter
spec:
  commonName: auth/cerbos-exporter
  dnsNames:
    - cerbos-exporter.auth
  duration: 24h
  renewBefore: 3h
  issuerRef:
    kind: ClusterIssuer
    name: kafka-shared-selfsigned-issuer
  secretName: iam-cerbos-exporter-cert
```

and then mount them to the pod:

```yaml
      volumeMounts:
        - name: kafka-client-cert
          mountPath: /certs
          readOnly: true
  volumes:
    - name: kafka-client-cert
      secret:
        secretName: iam-cerbos-exporter-cert
```
